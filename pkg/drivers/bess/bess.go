// Copyright (c) 2019 Red Hat and/or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bess contains the BESS data plane driver for Nimbess Agent
package bess

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/nimbess/nimbess-agent/pkg/network"
	"github.com/nimbess/nimbess-agent/pkg/proto/bess_pb"
	"reflect"
	"strings"

	"github.com/nimbess/nimbess-agent/pkg/drivers"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// BESS specific object names
const (
	PORT      = "Port"
	MODULE    = "Module"
	VPORT     = "VPort"
	PORTOUT   = "PortOut"
	PORTINC   = "PortInc"
	L2FORWARD = "L2Forward"
	REPLICATE = "Replicate"
)

// SupportedObjects contains the BESS objects supported by this driver
var SupportedObjects = [...]string{PORT, MODULE}

// Driver represents the BESS driver for Nimbess Agent
type Driver struct {
	drivers.DriverConfig
	bessClient bess_pb.BESSControlClient
	Context    context.Context
}

// Connect is used to setup gRPC connection with the data plane.
func (d *Driver) Connect() *grpc.ClientConn {
	log.Info("Connecting to BESS")
	// TODO change this to unix socket connection
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", d.Port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	log.Info("Connected to BESS")
	d.bessClient = bess_pb.NewBESSControlClient(conn)
	verResponse, err := d.bessClient.GetVersion(context.Background(), &bess_pb.EmptyRequest{})
	if err != nil || verResponse.Error != nil {
		log.Fatalf("Could not get version: %v, %v", verResponse.GetError(), err)
	}
	log.Infof("BESS connected with version: %s", verResponse.Version)

	initWorkers(d.bessClient, d.WorkerCores)
	if err := d.resumeWorkers(); err != nil {
		log.Warning("Failed to start workers!")
	}
	return conn
}

func initWorkers(client bess_pb.BESSControlClient, workerCores []int64) {
	log.Info("Initializing worker cores")
	res, err := client.ListWorkers(context.Background(), &bess_pb.EmptyRequest{})
	if err != nil {
		log.Fatalf("Unable to list workers in BESS")
	} else if res.GetError().GetCode() != 0 {
		log.Fatal(res.GetError().GetErrmsg())
	}

	currentWorkersList := res.GetWorkersStatus()
	// find if existing workers match worker cores and delete them otherwise
	for _, cWorker := range currentWorkersList {
		workerExists := false
		for idx, workerCore := range workerCores {
			if workerCore == cWorker.GetCore() {
				workerCores = remove(workerCores, idx)
				workerExists = true
				break
			}
		}
		// delete unknown worker
		if !workerExists {
			log.Infof("Pausing and removing unknown worker %s on core: %s", cWorker.GetWid(), cWorker.GetCore())
			client.PauseWorker(context.Background(), &bess_pb.PauseWorkerRequest{cWorker.GetWid()})
			res, err := client.DestroyWorker(context.Background(),
				&bess_pb.DestroyWorkerRequest{cWorker.GetWid()})
			if err != nil {
				log.Errorf("Error destroying worker %s", cWorker.GetWid())
			} else if res.GetError().GetCode() != 0 {
				log.Errorf("Error destroying worker: %s", res.GetError().GetErrmsg())
			}
		}
	}

	// now add remaining workers that do not exist
	for _, newWorkerCore := range workerCores {
		log.Infof("Adding new worker for core %d", newWorkerCore)
		// Worker ID always matches core number
		res, err := client.AddWorker(context.Background(),
			&bess_pb.AddWorkerRequest{Wid: newWorkerCore, Core: newWorkerCore})
		if err != nil {
			log.Fatalf("Failed to add worker for core %d, %v", newWorkerCore, err)
		} else if res.GetError().GetCode() != 0 {
			log.Fatalf("Failed to add worker: %s", res.GetError().GetErrmsg())
		}
	}
}

func (d *Driver) resumeWorkers() error {
	res, err := d.bessClient.ResumeAll(context.Background(), &bess_pb.EmptyRequest{})
	if err != nil {
		log.Errorf("Unable to resume workers")
		return err
	} else if res.GetError().GetCode() != 0 {
		return errors.New(res.GetError().GetErrmsg())
	}
	return nil
}

func (d *Driver) pauseWorkers() error {
	res, err := d.bessClient.PauseAll(context.Background(), &bess_pb.EmptyRequest{})
	if err != nil {
		log.Errorf("Unable to pause workers")
		return err
	} else if res.GetError().GetCode() != 0 {
		return errors.New(res.GetError().GetErrmsg())
	}
	return nil
}

func remove(s []int64, i int) []int64 {
	s[i] = s[len(s)-1]
	// We do not need to put s[i] at the end, as it will be discarded anyway
	return s[:len(s)-1]
}

// RenderModules creates resources for modules and then wires them to pipeline
func (d *Driver) RenderModules(modules []network.PipelineModule) error {
	if modules == nil {
		return errors.New("cannot render module to pipeline. Module is null")
	}

	// Iterate modules and create each one first
	for _, module := range modules {
		// Figure out type of Module and proper create handler
		switch reflect.TypeOf(module) {
		case reflect.TypeOf(&network.IngressPort{}):
			// Create Module Egress port
			if err := d.createPortIncModule(module.(*network.IngressPort)); err != nil {
				return err
			}
		case reflect.TypeOf(&network.EgressPort{}):
			// Create Module Egress port
			if err := d.createPortOutModule(module.(*network.EgressPort)); err != nil {
				return err
			}
		case reflect.TypeOf(&network.Switch{}):
			// Create L2Forward and Replicator
			if err := d.createSwitch(module.(*network.Switch)); err != nil {
				return err
			}
		default:
			log.Errorf("Cannot render unknown Module %v", module)
			return errors.New("error rendering unknown module")
		}
	}

	// Connect each module to pipeline
	for _, module := range modules {
		// Do not wire forwarders because those are core of pipeline and should already be wired
		if reflect.TypeOf(module) == reflect.TypeOf(&network.Switch{}) {
			continue
		}
		if err := d.wireModule(module); err != nil {
			return err
		}
	}
	return nil

}

// createSwitch builds an L2Forward and Replicator BESS modules to represent a Nimbess Switch Forwarder
func (d *Driver) createSwitch(module *network.Switch) error {
	log.Debugf("Building Nimbess switch for: %+v", *module)
	// Create L2Forward module
	// Name uses module type extensions
	l2Fwd := &L2Forward{Switch: network.Switch{}}
	l2Fwd.SetName(fmt.Sprintf("%s_l2forward", module.GetName()))
	l2Fwd.IGates = module.IGates
	l2Fwd.EGates = make(map[network.Gate]network.PipelineModule)
	l2Fwd.L2FIB = module.L2FIB
	if err := d.createL2ForwardModule(*l2Fwd); err != nil {
		return err
	}
	// Create Replicator module for flooding
	rep := &Replicate{Switch: network.Switch{}}
	rep.SetName(fmt.Sprintf("%s_replicate", module.GetName()))
	rep.EGates = module.EGates
	// Egress gates from original Switch module will be used for L2Forward lookup match, and then
	// also used for replicator flooding. Add Egress Gate for L2Forward -> replicator and Ingress
	// gate for reverse
	l2Fwd.EGates[999] = rep
	// Replicate should have no Ingress gates other than l2fwd
	rep.IGates = make(map[network.Gate]network.PipelineModule)
	rep.IGates[0] = l2Fwd
	if err := d.createReplicateModule(*rep); err != nil {
		return err
	}

	// wire BESS ports
	if err := d.wireModule(l2Fwd); err != nil {
		return err
	}

	if err := d.wireModule(rep); err != nil {
		return err
	}

	log.Infof("BESS Switch created with L2FWD: %s, Replicate: %s", l2Fwd.Name, rep.Name)
	return nil

}

// createReplicateModule creates an instance of a Replicate in BESS
func (d *Driver) createReplicateModule(module Replicate) error {
	// Set gates based on egress mappings
	keys := make([]int64, 0, len(module.EGates))
	for k := range module.EGates {
		keys = append(keys, int64(k))
	}
	repArgs := &bess_pb.ReplicateArg{Gates: keys}
	any, err := ptypes.MarshalAny(repArgs)
	if err != nil {
		log.Errorf("Failure to serialize replicate add args: %v", repArgs)
		return err
	}
	rep := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: REPLICATE,
		Arg:    any,
	}
	return d.createModule(rep)
}

// createL2ForwardModule creates an instance of L2Forward in BESS
func (d *Driver) createL2ForwardModule(module L2Forward) error {
	l2Args := &bess_pb.L2ForwardArg{
		Size:  d.DriverConfig.FIBSize,
		Learn: d.DriverConfig.MacLearn,
	}
	any, err := ptypes.MarshalAny(l2Args)
	if err != nil {
		log.Errorf("Failure to serialize L2 add args: %v", l2Args)
		return err
	}
	l2module := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: L2FORWARD,
		Arg:    any,
	}

	return d.createModule(l2module)
}

// createModule creates any type of module within BESS
func (d *Driver) createModule(req *bess_pb.CreateModuleRequest) error {
	cRes, err := d.bessClient.CreateModule(context.Background(), req)
	if err != nil {
		log.Errorf("Failed to create %s: %v", req.Mclass, err)
		return err
	} else if cRes.Error.GetCode() != 0 {
		log.Errorf("Failed to create %s: %s", req.Mclass, cRes.Error)
		return errors.New(cRes.GetError().GetErrmsg())
	} else {
		log.Infof("Module created: %s, %s", cRes.Name, req.Mclass)
	}

	return nil
}

// wireModule connects a BESS module to others based on gate linkage
func (d *Driver) wireModule(module network.PipelineModule) error {
	log.Debugf("Wiring up module: %+v", module)
	// Wire ingress gates first: other_module<M1> (eGate) --> (iGate) this_module<M2>
	iGates := module.GetIGateMap()
	for iGate, mod1 := range iGates {
		log.Debugf("Wiring Ingress gate %d, peering module: %s", iGate, mod1.GetName())
		matchingConn := false
		// We now need to get eGate for connecting module
		var eGate network.Gate
		realModName := mod1.GetName()
		// If module is a Switch we know in BESS this next module must be an l2forward so fix name
		if reflect.TypeOf(mod1) == reflect.TypeOf(&network.Switch{}) {
			realModName = fmt.Sprintf("%s_replicate", mod1.GetName())
		}
		for gate, mod2 := range mod1.GetEGateMap() {
			if mod2 == module {
				eGate = gate
				log.Debugf("Matching Egate found for module: %s, gate %d", mod1.GetName(), eGate)
				matchingConn = true
				break
			}
		}

		if matchingConn {
			connReq := &bess_pb.ConnectModulesRequest{
				M1:    realModName,
				M2:    module.GetName(),
				Igate: uint64(iGate),
				Ogate: uint64(eGate),
			}
			if !d.connExists(connReq) {
				if err := d.connectModules(connReq); err != nil {
					return err
				}
			}
		}
	}
	log.Debugf("Completed wiring ingress gates for module %s", module.GetName())

	// Wire egress gates: this_module<M1> (eGate) --> (iGate) other_module<M2>
	eGates := module.GetEGateMap()
	for eGate, mod2 := range eGates {
		log.Debugf("Wiring Egress gate %d, peering module: %s", eGate, mod2.GetName())
		matchingConn := false
		// We now need to get iGate for connecting module
		var iGate network.Gate
		realModName := mod2.GetName()
		// If module is a Switch we know in BESS this previous module must be a replicate so fix name
		if reflect.TypeOf(mod2) == reflect.TypeOf(&network.Switch{}) {
			realModName = fmt.Sprintf("%s_l2forward", mod2.GetName())
		}
		for gate, mod1 := range mod2.GetIGateMap() {
			if mod1 == module {
				iGate = gate
				log.Debugf("Matching Igate found for module: %s, gate %d", mod2.GetName(), iGate)
				matchingConn = true
				break
			}
		}

		if matchingConn {
			connReq := &bess_pb.ConnectModulesRequest{
				M1:    module.GetName(),
				M2:    realModName,
				Igate: uint64(iGate),
				Ogate: uint64(eGate),
			}
			if !d.connExists(connReq) {
				if err := d.connectModules(connReq); err != nil {
					return err
				}
			}
		}
	}
	log.Infof("Gates all wired for module %s", module.GetName())
	return nil
}

// connExists checks if a connection exists between two modules
func (d *Driver) connExists(connReq *bess_pb.ConnectModulesRequest) bool {
	log.Info("Checking if connection already exists: %+v", connReq)
	res, err := d.bessClient.GetModuleInfo(d.Context, &bess_pb.GetModuleInfoRequest{Name: connReq.M1})
	if err != nil {
		log.Errorf("Error while trying to check connection: %v", err)
		return false
	}
	oGates := res.GetOgates()
	if oGates == nil {
		return false
	}
	for _, oGate := range oGates {
		if oGate.GetOgate() == connReq.Ogate {
			log.Debugf("Output gate found. Input gate: %d, Name: %s", oGate.GetIgate(), oGate.GetName())
			if oGate.GetOgate() == connReq.GetOgate() && oGate.GetName() == connReq.M2 {
				log.Info("Connection exists for %+v", connReq)
				return true
			}
		}
	}
	return false
}

// connectModules handles sending the connection request to BESS
func (d *Driver) connectModules(connReq *bess_pb.ConnectModulesRequest) error {
	log.Infof("Creating connection: %+v", connReq)
	conRes, err := d.bessClient.ConnectModules(d.Context, connReq)
	if err != nil {
		log.Errorf("Failed to connect modules: %s", err.Error())
		return err
	} else if conRes.Error.GetCode() != 0 {
		log.Errorf("Failed to connect modules: %s", conRes.GetError().Errmsg)
		return errors.New(conRes.GetError().Errmsg)
	} else {
		log.Infof("Connection created via request: %v", connReq)
	}
	return nil
}

// createPortIncModule creates an Ingress port in BESS
func (d *Driver) createPortIncModule(module *network.IngressPort) error {
	log.Debugf("Creating BESS Ingress port: %s", module.Name)
	// Check if BESS Port exists, if not create it
	exists, err := objectExists(d.bessClient, module.PortName, PORT)
	if err != nil {
		log.Warningf("Unable to check if object %s exists...assuming it does not", module.PortName)
	}
	if !exists {
		// Create Port
		log.Debugf("Port %s missing, creating...", module.PortName)
		err = d.createPort(module.Port)
	}

	// Create PortInc Module
	portArgs := &bess_pb.PortIncArg{
		Port: module.PortName,
	}
	any, err := ptypes.MarshalAny(portArgs)
	if err != nil {
		log.Errorf("Failure to serialize port args: %v", portArgs)
		return err
	}
	portReq := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: PORTINC,
		Arg:    any,
	}
	return d.createModule(portReq)
}

// createPortOutModule creates an Egress port in BESS
func (d *Driver) createPortOutModule(module *network.EgressPort) error {
	log.Debugf("Creating BESS Egress port: %s", module.Name)
	// Check if BESS Port exists, if not create it
	exists, err := objectExists(d.bessClient, module.PortName, PORT)
	if err != nil {
		log.Warningf("Unable to check if object %s exists...assuming it does not", module.PortName)
	}
	if !exists {
		// Create Port
		log.Debugf("Port %s missing, creating...", module.PortName)
		err = d.createPort(module.Port)
	}

	// Create PortOut Module
	portArgs := &bess_pb.PortOutArg{
		Port: module.PortName,
	}
	any, err := ptypes.MarshalAny(portArgs)
	if err != nil {
		log.Errorf("Failure to serialize port args: %v", portArgs)
		return err
	}
	portReq := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: PORTOUT,
		Arg:    any,
	}
	return d.createModule(portReq)
}

// createPort creates a Port object in BESS and updates the Nimbess port
func (d *Driver) createPort(port *network.Port) error {
	var portDriver string
	if port.Virtual {
		portDriver = VPORT
	} else {
		return errors.New("only virtual ports currently supported")
	}

	if port.DPDK {
		return errors.New("DPDK ports are not currently supported")
	}

	vportArg := &bess_pb.VPortArg_Netns{
		Netns: port.NamesSpace,
	}
	vportArgs := &bess_pb.VPortArg{
		Ifname:  port.IfaceName,
		Cpid:    vportArg,
		IpAddrs: []string{port.IPAddr},
	}
	any, err := ptypes.MarshalAny(vportArgs)
	if err != nil {
		log.Errorf("Failure to serialize vport args: %v", vportArgs)
		return errors.New("failed to serialize port args")
	}
	portRequest := &bess_pb.CreatePortRequest{
		Name:   port.PortName,
		Driver: portDriver,
		Arg:    any,
	}
	portRes, err := d.bessClient.CreatePort(d.Context, portRequest)

	if err != nil || portRes.Error.Errmsg != "" {
		log.Errorf("Failure to create port with Bess: %v, %v", portRes, err)
		return err
	}
	// Update mac address for port
	port.MacAddr = portRes.GetMacAddr()
	return nil
}

/* Checks if BESS resource exists. oType is BESS object type which must be one of SupportedObjects types
 */
func objectExists(client bess_pb.BESSControlClient, object string, oType string) (bool, error) {
	objectSupported := false
	for _, suppObject := range SupportedObjects {
		if oType == suppObject {
			objectSupported = true
			break
		}
	}

	if !objectSupported {
		return false, fmt.Errorf("unsupported object type: %s", oType)
	}

	moduleName := strings.Join([]string{"List", oType, "s"}, "")
	inputs := make([]reflect.Value, 2)
	inputs[0] = reflect.ValueOf(context.Background())
	inputs[1] = reflect.ValueOf(&bess_pb.EmptyRequest{})
	method := reflect.ValueOf(client).MethodByName(moduleName)
	retVals := method.Call(inputs)

	if retVals[1].Interface() != nil {
		log.Errorf("Error querying for object: %s, %v", object, retVals[1].Interface().(error))
		return false, retVals[1].Interface().(error)
	}

	resp := retVals[0]
	respType := resp.Type()
	switch respType.String() {
	case "*bess_pb.ListModulesResponse":
		modules := resp.Interface().(*bess_pb.ListModulesResponse).GetModules()
		for _, module := range modules {
			if module.Name == object {
				return true, nil
			}
		}
	case "*bess_pb.ListPortsResponse":
		ports := resp.Interface().(*bess_pb.ListPortsResponse).GetPorts()
		for _, port := range ports {
			if port.Name == object {
				return true, nil
			}
		}
	}

	return false, nil
}

// AddEntryL2FIB adds an L2 entry (Destination MAC address and output gate) to a Switch
func (d *Driver) AddEntryL2FIB(module *network.Switch, macAddr string, gate network.Gate) error {
	log.Infof("Creating L2 BESS entry for mac: %s, gate: %d", macAddr, gate)
	entry := &bess_pb.L2ForwardCommandAddArg_Entry{Addr: macAddr, Gate: int64(gate)}
	addArg := &bess_pb.L2ForwardCommandAddArg{Entries: []*bess_pb.L2ForwardCommandAddArg_Entry{entry}}
	any, err := ptypes.MarshalAny(addArg)
	if err != nil {
		log.Errorf("Failure to serialize L2 add args: %v", addArg)
		return err
	}
	l2FwdName := fmt.Sprintf("%s_l2forward", module.GetName())
	cmd := &bess_pb.CommandRequest{Name: l2FwdName, Cmd: "add", Arg: any}
	res, err := d.bessClient.ModuleCommand(context.Background(), cmd)
	if err != nil {
		log.Errorf("Failed to add L2FIB entry: %v, error: %v", entry, err)
		return err
	} else if res.GetError().GetCode() != 0 {
		log.Errorf("Failed to add tcam entry, error: %s", res.GetError().GetErrmsg())
		return errors.New(res.GetError().GetErrmsg())
	}
	log.Infof("BESS L2 FIB entry created: %+v", entry)
	return nil
}
