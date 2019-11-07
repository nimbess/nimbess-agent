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
	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/uuid"
	"github.com/nimbess/nimbess-agent/pkg/network"
	"github.com/nimbess/nimbess-agent/pkg/proto/bess_pb"
	"net/url"
	"path"
	"reflect"
	"strings"

	"github.com/nimbess/nimbess-agent/pkg/drivers"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// BESS specific object names
const (
	PORT        = "Port"
	MODULE      = "Module"
	VPORT       = "VPort"
	PMDPORT     = "PMDPort"
	PCAPPORT    = "PCAPPort"
	UNIXPORT    = "UnixSocketPort"
	PORTOUT     = "PortOut"
	PORTINC     = "PortInc"
	L2FORWARD   = "L2Forward"
	REPLICATE   = "Replicate"
	SOCKET_PATH = "/var/lib/nimbess/cni/u-%s"
	URLFILTER   = "UrlFilter"
)

// SupportedObjects contains the BESS objects supported by this driver
var SupportedObjects = [...]string{PORT, MODULE}

// Driver represents the BESS driver for Nimbess Agent
type Driver struct {
	drivers.DriverConfig
	bessClient    bess_pb.BESSControlClient
	Context       context.Context
	notifications chan network.L2FIBCommand
	socketmap     map[string]string
}

func NewDriver(configArg drivers.DriverConfig, contextArg context.Context) *Driver {
	return &Driver{
		DriverConfig:  configArg,
		Context:       contextArg,
		notifications: make(chan network.L2FIBCommand),
		socketmap:     make(map[string]string),
	}
}

// map various socket paths onto a semirandom sequence which will fit
// path length restriction

func (d *Driver) socketMapEntry(arg string) string {
	if entry, err := d.socketmap[arg]; err {
		return entry
	}
	entry, _ := uuid.NewRandom()
	d.socketmap[arg] = entry.String()
	return d.socketmap[arg]
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

func (d *Driver) GetNotifications() chan network.L2FIBCommand {
	return d.notifications
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
	log.Debugf("Rendering Nimbess pipeline: %+v", modules)
	if modules == nil {
		return errors.New("cannot render module to pipeline. Module is null")
	}

	// Iterate modules and create each one first
	for _, module := range modules {
		// Figure out type of Module and proper create handler
		switch reflect.TypeOf(module) {
		case nil:
			continue
		case reflect.TypeOf(&network.IngressPort{}):
			if exists, _ := objectExists(d.bessClient, module.GetName(), MODULE); exists == true {
				log.Debugf("Skipping render for %s, already exists", module.GetName())
				continue
			}
			// Create Module Ingress port
			if err := d.createPortIncModule(module.(*network.IngressPort)); err != nil {
				return err
			}
		case reflect.TypeOf(&network.EgressPort{}):
			if exists, _ := objectExists(d.bessClient, module.GetName(), MODULE); exists == true {
				log.Debugf("Skipping render for %s, already exists", module.GetName())
				continue
			}
			// Create Module Egress port
			if err := d.createPortOutModule(module.(*network.EgressPort)); err != nil {
				return err
			}
		case reflect.TypeOf(&network.Switch{}):
			// TODO(trozet) handle checking if switch already exists. This shouldn't happen
			// Create L2Forward and Replicator
			if err := d.createSwitch(module.(*network.Switch)); err != nil {
				return err
			}
		case reflect.TypeOf(&network.URLFilter{}):
			if exists, _ := objectExists(d.bessClient, module.GetName(), MODULE); exists == true {
				log.Debugf("Skipping render for %s, already exists", module.GetName())
				continue
			}
			if err := d.createUrlFilter(module.(*network.URLFilter)); err != nil {
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
		if reflect.TypeOf(module) == nil {
			continue
		}
		if err := d.wireModule(module); err != nil {
			return err
		}
	}
	return nil

}

// ReRenderModules updates pipeline resources based on a change to the pipeline
// This should detect changes and update only necessary pieces, but for now it deletes and re-renders the pipeline
func (d *Driver) ReRenderModules(modules []network.PipelineModule) error {
	if modules == nil {
		return errors.New("cannot render module to pipeline. Modules is null")
	}

	// Delete all modules in the port pipeline except for egress ports
	log.Infof("Deleting pipeline: %+v", modules)
	if err := d.DeleteModules(modules, false); err != nil {
		return err
	}
	return d.RenderModules(modules)
}

// createSwitch builds an L2Forward and Replicator BESS modules to represent a Nimbess Switch Forwarder
func (d *Driver) createSwitch(module *network.Switch) error {
	log.Debugf("Building Nimbess switch for: %+v", *module)
	// Create L2Forward module
	// Name uses module type extensions
	l2Fwd := &L2Forward{Switch: network.Switch{}}
	l2Fwd.SetName(fmt.Sprintf("%s_l2forward", module.GetName()))
	l2Fwd.IGates = module.IGates
	l2Fwd.EGates = network.MakeGateMap()
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
	l2Fwd.EGates[0] = rep
	// Replicate should have no Ingress gates other than l2fwd
	rep.IGates = network.MakeGateMap()
	rep.IGates[0] = l2Fwd
	if err := d.createReplicateModule(*rep); err != nil {
		return err
	}
	port := &network.Port{
		PortName:   fmt.Sprintf("%s_monitor", module.GetName()),
		Virtual:    false,
		DPDK:       false,
		UnixSocket: true,
		SocketPath: fmt.Sprintf(SOCKET_PATH, d.socketMapEntry(module.GetName())),
	}
	if err := d.createPort(port); err != nil {
		return err
	}
	monitor := &network.EgressPort{
		Port: port,
	}
	monitor.SetName(port.PortName)
	monitor.IGates = network.MakeGateMap()
	monitor.IGates[rep.GetNextEGate()] = rep
	rep.EGates[rep.GetNextEGate()] = monitor

	if err := d.createPortOutModule(monitor); err != nil {
		return err
	}

	// wire BESS ports
	if err := d.wireModule(l2Fwd); err != nil {
		return err
	}

	if err := d.wireModule(rep); err != nil {
		return err
	}

	if err := d.wireModule(monitor); err != nil {
		return err
	}
	// Set default gate for l2forward to 0
	gateArg := &bess_pb.L2ForwardCommandSetDefaultGateArg{Gate: 0}
	gateAny, err := ptypes.MarshalAny(gateArg)
	if err != nil {
		log.Errorf("Failure to serialize default gate arg: %v", gateArg)
		return err
	}
	cmdReq := &bess_pb.CommandRequest{Name: l2Fwd.GetName(), Cmd: "set_default_gate", Arg: gateAny}
	res, err := d.bessClient.ModuleCommand(d.Context, cmdReq)
	if err != nil {
		log.Errorf("Failed to set default gate for l2forward, error: %v", err)
		return err
	} else if res.GetError().GetCode() != 0 {
		log.Errorf("Failed to set default gate for l2forward, error: %s", res.GetError().GetErrmsg())
		return errors.New(res.GetError().GetErrmsg())
	}
	// We need to recover the original mod name from Switch_%s
	origName := module.GetName()
	metaName := module.GetMeta() //expected as Switch_<meta name> - from pipeline module
	if len(metaName) == 0 {
		log.Warningf("Meta pipeline empty for Switch: %s", origName)
	}
	derivedPortName := origName[len(metaName)+1:] // remove "<MetaName>_" prefix
	log.Debugf("Module name: %s, meta name: %s, port name: %s", origName, metaName, derivedPortName)
	r := NewReader(derivedPortName, port.SocketPath, d.notifications)

	go r.Run()
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
	repAny, err := ptypes.MarshalAny(repArgs)
	if err != nil {
		log.Errorf("Failure to serialize replicate add args: %v", repArgs)
		return err
	}
	rep := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: REPLICATE,
		Arg:    repAny,
	}
	return d.createModule(rep)
}

// createL2ForwardModule creates an instance of L2Forward in BESS
func (d *Driver) createL2ForwardModule(module L2Forward) error {
	l2Args := &bess_pb.L2ForwardArg{
		Size:  d.DriverConfig.FIBSize,
		Learn: d.DriverConfig.MacLearn,
	}
	l2Any, err := ptypes.MarshalAny(l2Args)
	if err != nil {
		log.Errorf("Failure to serialize L2 add args: %v", l2Args)
		return err
	}
	l2module := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: L2FORWARD,
		Arg:    l2Any,
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
		if mod1 == nil {
			// There is little point in explicit wiring a null igate to let's say DROP
			continue
		}
		log.Debugf("Wiring Ingress gate %d, peering module: %s", iGate, mod1.GetName())
		matchingConn := false
		// We now need to get eGate for connecting module
		var eGate network.Gate
		var realModNames []string
		realModName := mod1.GetName()
		// If module is a Switch we know in BESS this M2 must be wired to the replicate and forward parts of Switch
		// for egress traffic
		if reflect.TypeOf(mod1) == reflect.TypeOf(&network.Switch{}) {
			realModNames = append(realModNames, fmt.Sprintf("%s_replicate", mod1.GetName()))
			realModNames = append(realModNames, fmt.Sprintf("%s_l2forward", mod1.GetName()))
		} else {
			realModNames = append(realModNames, realModName)
		}

		for _, modName := range realModNames {
			for gate, mod2 := range mod1.GetEGateMap() {
				if mod2 == module {
					eGate = gate
					log.Debugf("Matching Egate found for module: %s, gate %d", modName, eGate)
					matchingConn = true
					break
				}
			}

			if matchingConn {
				connReq := &bess_pb.ConnectModulesRequest{
					M1:    modName,
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
		// If module was a switch and we wired up a new egress connection, we need to update replication gates
		if reflect.TypeOf(mod1) == reflect.TypeOf(&network.Switch{}) {
			// figure out replication gates and update them
			gates := make([]int64, len(mod1.GetEGateMap()))
			i := 0
			for k := range mod1.GetEGateMap() {
				gates[i] = int64(k)
				i++
			}
			repArg := &bess_pb.ReplicateCommandSetGatesArg{Gates: gates}
			repAny, err := ptypes.MarshalAny(repArg)
			if err != nil {
				log.Errorf("Failure to serialize rep gate args: %v", repArg)
				return err
			}
			cmdReq := &bess_pb.CommandRequest{Name: fmt.Sprintf("%s_replicate", mod1.GetName()),
				Cmd: "set_gates", Arg: repAny}
			res, err := d.bessClient.ModuleCommand(d.Context, cmdReq)
			if err != nil {
				log.Errorf("Failed to set replication gates, error: %v", err)
				return err
			} else if res.GetError().GetCode() != 0 {
				log.Errorf("Failed to set replication gates, error: %s", res.GetError().GetErrmsg())
				return errors.New(res.GetError().GetErrmsg())
			}
			log.Debugf("Replication gates updated for switch %s, %v", mod1.GetName(), gates)
		}
	}
	log.Debugf("Completed wiring ingress gates for module %s", module.GetName())

	// Wire egress gates: this_module<M1> (eGate) --> (iGate) other_module<M2>
	eGates := module.GetEGateMap()
	for eGate, mod2 := range eGates {
		if mod2 == nil {
			continue
		}
		log.Debugf("Wiring Egress gate %d, peering module: %s", eGate, mod2.GetName())
		matchingConn := false
		// We now need to get iGate for connecting module
		var iGate network.Gate
		realModName := mod2.GetName()
		// If mod2 is a Switch we know in BESS this next module must be a l2forward so fix name
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
	portAny, err := ptypes.MarshalAny(portArgs)
	if err != nil {
		log.Errorf("Failure to serialize port args: %v", portArgs)
		return err
	}
	portReq := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: PORTINC,
		Arg:    portAny,
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
	portAny, err := ptypes.MarshalAny(portArgs)
	if err != nil {
		log.Errorf("Failure to serialize port args: %v", portArgs)
		return err
	}
	portReq := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: PORTOUT,
		Arg:    portAny,
	}
	return d.createModule(portReq)
}

// createUrlFilter creates an URL Filter module in BESS
func (d *Driver) createUrlFilter(module *network.URLFilter) error {
	log.Debugf("Creating BESS URL Filter: %s", module.Name)
	var blackList []*bess_pb.UrlFilterArg_Url

	for _, site := range module.Urls {
		log.Debugf("Adding site to URL Filter: %s", site)
		// check if URL has a scheme. If it does then can use url parse
		u, uErr := url.Parse(site)
		if strings.Contains(site, ":") && uErr == nil {
			log.Debugf("Appending URL to blacklist, host: %s, path: %s", u.Host, u.Path)
			blackList = append(blackList, &bess_pb.UrlFilterArg_Url{
				Host: u.Host,
				Path: u.Path,
			})
		} else {
			paths := strings.SplitN(site, "/", 2)
			if len(paths) == 0 {
				log.Warnf("Skipping site: %s, failed to parse as it is empty", site)
				continue
			} else if len(paths) == 1 {
				host := paths[0]
				log.Debugf("Appending site to blacklist, host: %s, path: none", host)
				blackList = append(blackList, &bess_pb.UrlFilterArg_Url{
					Host: host,
				})
			} else {
				host := paths[0]
				path := fmt.Sprintf("/%s", paths[1])
				log.Debugf("Appending site to blacklist, host: %s, path: %s", host, path)
				blackList = append(blackList, &bess_pb.UrlFilterArg_Url{
					Host: host,
					Path: path,
				})
			}
		}
	}
	// Create UrlFilter Module
	urlArgs := &bess_pb.UrlFilterArg{
		Blacklist: blackList,
	}
	urlAny, err := ptypes.MarshalAny(urlArgs)
	if err != nil {
		log.Errorf("Failure to serialize port args: %v", urlArgs)
		return err
	}
	portReq := &bess_pb.CreateModuleRequest{
		Name:   module.Name,
		Mclass: URLFILTER,
		Arg:    urlAny,
	}
	return d.createModule(portReq)
}

// createPort creates a Port object in BESS and updates the Nimbess port
func (d *Driver) createPort(port *network.Port) error {
	var portAny *any.Any
	var err error
	var portDriver string

	if port.Virtual {
		if port.DPDK {
			portDriver = PMDPORT
			vdevArg := &bess_pb.PMDPortArg_Vdev{
				Vdev: fmt.Sprintf("%s,iface=%s", port.IfaceName, port.SocketPath),
			}
			pmdPortArg := &bess_pb.PMDPortArg{Port: vdevArg}
			portAny, err = ptypes.MarshalAny(pmdPortArg)
			if err != nil {
				log.Errorf("Failure to serialize PMD port args: %v", pmdPortArg)
				return errors.New("failed to serialize port args")
			}
		} else {
			var vportArgs *bess_pb.VPortArg
			portDriver = VPORT
			if port.NamesSpace == "" {
				// If no namespace, this must belong to default namespace so just provide IF Name and IP
				vportArgs = &bess_pb.VPortArg{
					Ifname:  port.IfaceName,
					IpAddrs: []string{port.IPAddr},
				}
			} else {
				// prepend /host dir as this is what is mounted to container
				realNsPath := path.Join("/host", port.NamesSpace)
				vportArg := &bess_pb.VPortArg_Netns{
					Netns: realNsPath,
				}
				vportArgs = &bess_pb.VPortArg{
					Ifname:  port.IfaceName,
					Cpid:    vportArg,
					IpAddrs: []string{port.IPAddr},
				}
			}
			portAny, err = ptypes.MarshalAny(vportArgs)
			if err != nil {
				log.Errorf("Failure to serialize vport args: %v", vportArgs)
				return errors.New("failed to serialize port args")
			}
		}
	} else {
		if port.DPDK {
			portDriver = PMDPORT
			pciArg := &bess_pb.PMDPortArg_Pci{Pci: port.IfaceName}
			portArg := &bess_pb.PMDPortArg{Port: pciArg}
			portAny, err = ptypes.MarshalAny(portArg)
			if err != nil {
				log.Errorf("Failure to serialize PMD Port port args: %v", portArg)
				return errors.New("failed to serialize port args")
			}

		} else if port.UnixSocket {
			portDriver = UNIXPORT
			portArg := &bess_pb.UnixSocketPortArg{Path: port.SocketPath}
			portAny, err = ptypes.MarshalAny(portArg)
			if err != nil {
				log.Errorf("Failure to serialize Unix Socket port args: %v", portArg)
				return errors.New("failed to serialize port args")
			}
		} else {
			// Must be a kernel iface, use PCAP type port
			portDriver = PCAPPORT
			portArg := &bess_pb.PCAPPortArg{Dev: port.IfaceName}
			portAny, err = ptypes.MarshalAny(portArg)
			if err != nil {
				log.Errorf("Failure to serialize pcap port args: %v", portArg)
				return errors.New("failed to serialize port args")
			}
		}
	}

	portRequest := &bess_pb.CreatePortRequest{
		Name:   port.PortName,
		Driver: portDriver,
		Arg:    portAny,
	}
	log.Debugf("Requesting a port create for: %v, %v, %+v", port.PortName, portDriver, portAny)
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
		log.Errorf("Check for unsupported object type: %s", oType)
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
			log.Debugf("Iterating through module: %s", module.Name)
			if module.Name == object {
				log.Debugf("Object found: %s of type %s", object, oType)
				return true, nil
			}
		}
	case "*bess_pb.ListPortsResponse":
		ports := resp.Interface().(*bess_pb.ListPortsResponse).GetPorts()
		for _, port := range ports {
			log.Debugf("Iterating through port: %s", port.Name)
			if port.Name == object {
				log.Debugf("Object found: %s of type %s", object, oType)
				return true, nil
			}
		}
	}
	log.Debugf("Object: %s of type %s does not exist", object, oType)
	return false, nil
}

// AddEntryL2FIB adds an L2 entry (Destination MAC address and output gate) to a Switch
func (d *Driver) AddEntryL2FIB(module *network.Switch, macAddr string, gate network.Gate) error {
	log.Infof("Creating L2 BESS entry for mac: %s, gate: %d", macAddr, gate)
	entry := &bess_pb.L2ForwardCommandAddArg_Entry{Addr: macAddr, Gate: int64(gate)}
	addArg := &bess_pb.L2ForwardCommandAddArg{Entries: []*bess_pb.L2ForwardCommandAddArg_Entry{entry}}
	FIBAny, err := ptypes.MarshalAny(addArg)
	if err != nil {
		log.Errorf("Failure to serialize L2 add args: %v", addArg)
		return err
	}
	l2FwdName := fmt.Sprintf("%s_l2forward", module.GetName())
	cmd := &bess_pb.CommandRequest{Name: l2FwdName, Cmd: "add", Arg: FIBAny}
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

// AddEntryL2FIB adds an L2 entry (Destination MAC address and output gate) to a Switch
func (d *Driver) DelEntryL2FIB(module *network.Switch, macAddr string) error {
	log.Infof("Deleting L2 BESS entry for mac: %s", macAddr)
	entry := []string{macAddr}
	delArg := &bess_pb.L2ForwardCommandDeleteArg{Addrs: entry}
	FIBAny, err := ptypes.MarshalAny(delArg)
	if err != nil {
		log.Errorf("Failure to serialize L2 delete args: %v", delArg)
		return err
	}
	l2FwdName := fmt.Sprintf("%s_l2forward", module.GetName())
	cmd := &bess_pb.CommandRequest{Name: l2FwdName, Cmd: "delete", Arg: FIBAny}
	res, err := d.bessClient.ModuleCommand(context.Background(), cmd)
	if err != nil {
		log.Errorf("Failed to delete L2FIB entry: %v, error: %v", entry, err)
		return err
	} else if res.GetError().GetCode() != 0 {
		log.Errorf("Failed to delete tcam entry, error: %s", res.GetError().GetErrmsg())
		return errors.New(res.GetError().GetErrmsg())
	}
	log.Infof("BESS L2 FIB entry deleted: %+v", entry)
	return nil
}

// deleteModule handles deleting a module by name in BESS. Ignores modules that do not exist.
// name is name of module to delete
func (d *Driver) deleteModule(name string) error {
	exists, err := objectExists(d.bessClient, name, MODULE)
	if err != nil {
		log.Warningf("Skipping delete. Unable to query for BESS module: %s, error: %v", name, err)
		return nil
	} else if !exists {
		log.Debugf("Skipping delete for BESS module: %s, module does not exist.", name)
		return nil
	}
	res, err := d.bessClient.DestroyModule(context.Background(),
		&bess_pb.DestroyModuleRequest{Name: name})
	if err != nil {
		log.Errorf("Failed to delete %s: %v", name, err)
		return err
	} else if res.Error.GetCode() != 0 {
		log.Errorf("Failed to delete %s: %s", name, res.Error)
		return errors.New(res.GetError().GetErrmsg())
	} else {
		log.Infof("Module deleted: %s", name)
	}
	return nil
}

// DeleteModules deletes all modules if they exist in BESS
// When egress is false, egress ports will be ignored
func (d *Driver) DeleteModules(modules []network.PipelineModule, egress bool) error {
	log.Infof("Deleting BESS modules, egress: %t, modules: %v", egress, modules)
	for _, module := range modules {
		// If pipeline module is switch we need to translate Nimbess Switch into BESS modules
		if reflect.TypeOf(module) == reflect.TypeOf(&network.Switch{}) {
			log.Debug("Switch detected, deleting l2forward module")
			if err := d.deleteModule(fmt.Sprintf("%s_l2forward", module.GetName())); err != nil {
				return err
			}
			log.Debug("Switch detected, deleting replicator module")
			if err := d.deleteModule(fmt.Sprintf("%s_replicate", module.GetName())); err != nil {
				return err
			}
			log.Debug("Switch detected, deleting monitor PortOut")
			if err := d.deleteModule(fmt.Sprintf("%s_monitor", module.GetName())); err != nil {
				return err
			}
			log.Debug("Switch detected, deleting monitor Port")
			if err := d.DeletePort(fmt.Sprintf("%s_monitor", module.GetName())); err != nil {
				return err
			}
		} else if reflect.TypeOf(module) == reflect.TypeOf(&network.EgressPort{}) && !egress {
			continue
		} else {
			if err := d.deleteModule(module.GetName()); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeletePort deletes a specific port if it exists in BESS
func (d *Driver) DeletePort(name string) error {
	log.Infof("Deleting BESS port: %s", name)
	exists, err := objectExists(d.bessClient, name, PORT)
	if err != nil {
		log.Warningf("Skipping delete. Unable to query for BESS port: %s, error: %v", name, err)
		return nil
	} else if !exists {
		log.Debugf("Skipping delete for BESS port: %s, port does not exist.", name)
		return nil
	}

	res, err := d.bessClient.DestroyPort(context.Background(),
		&bess_pb.DestroyPortRequest{Name: name})
	if err != nil {
		log.Errorf("Failed to delete %s: %v", name, err)
		return err
	} else if res.Error.GetCode() != 0 {
		log.Errorf("Failed to delete %s: %s", name, res.Error)
		return errors.New(res.GetError().GetErrmsg())
	} else {
		log.Infof("Module deleted: %s", name)
	}
	return nil
}

// Commit handles enabling config in data plane. In BESS for now we just pause/resume
func (d *Driver) Commit() error {
	log.Debug("Pausing all workers in BESS")
	if _, err := d.bessClient.PauseAll(d.Context, &bess_pb.EmptyRequest{}); err != nil {
		log.Warningf("Unable to Pause workers in BESS: %v", err)
	}
	log.Debug("Resuming all workers in BESS")
	if _, err := d.bessClient.ResumeAll(d.Context, &bess_pb.EmptyRequest{}); err != nil {
		log.Errorf("Unable to Resume workers in BESS: %v", err)
		return err
	}
	return nil
}
