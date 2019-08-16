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

// Agent runtime

package agent

import (
	"errors"
	"fmt"
	"github.com/containernetworking/plugins/pkg/ip"
	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/google/uuid"
	"github.com/nimbess/nimbess-agent/pkg/network"
	log "github.com/sirupsen/logrus"
	"github.com/vishvananda/netlink"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"path"
	"reflect"
	"sync"

	"github.com/nimbess/nimbess-agent/pkg/drivers"
	"github.com/nimbess/nimbess-agent/pkg/proto/cni"
)

// CNI Reply Values
const (
	CniOk                = 0
	CniIncompatible      = 1
	InvalidNetworkConfig = 2
	ContainerNotExist    = 3
	CniTryLater          = 11
	DriverFailure        = 101
	InternalIPAM         = "nimbess"
)

// TODO REMOVE this when IPAM is supported from CNI side
var ipAddr = net.IP{40, 0, 0, 0}

// NimbessAgent represents the agent runtime server.
// It contains a loadable runtime data plane driver to manage the data plane.
// It includes a mutex used to handle locking between driver and agent events to force a
// single-processed event pipeline.
type NimbessAgent struct {
	ID     uuid.UUID
	Mu     *sync.Mutex
	Config *NimbessConfig
	Driver drivers.Driver
	// Pipelines contains a map of port name to pipeline pointer
	Pipelines map[string]*NimbessPipeline
	// MetaPipelines contains a map of network name to abstract pipelines
	MetaPipelines map[string]*NimbessPipeline
}

// getPortName translates a CNI request into a Nimbess Port Identifier
func getPortName(req *cni.CNIRequest) string {
	return fmt.Sprintf("port_%s_%s", req.ContainerId, req.InterfaceName)
}

// invokes IPAM either internal or external and returns the CIDR
func invokeIPAM(ipamType string) (string, error) {
	if ipamType == InternalIPAM {
		ipAddr[3]++
		return fmt.Sprintf("%s/24", ipAddr.String()), nil
	}
	return "", fmt.Errorf("external IPAM types are not yet supported: %s", ipamType)
}

func (s *NimbessAgent) createPortPipeline(port *network.Port, metaKey string) (*cni.CNIReply, error) {
	portName := port.PortName
	// Init Port Pipeline
	s.Pipelines[portName] = &NimbessPipeline{Mu: s.Mu, Driver: s.Driver, MetaKey: metaKey,
		Name: fmt.Sprintf("%s_pipeline", portName), Modules: make([]network.PipelineModule, 0),
		EgressPorts: make([]*network.EgressPort, 0)}
	if err := s.Pipelines[portName].Init(portName, L2DriverMode, s.MetaPipelines[metaKey]); err != nil {
		log.Error("Error while initializing Port Pipeline: %v", err)
		return &cni.CNIReply{Result: CniIncompatible}, err
	}

	// Create Port in Nimbess Pipeline
	eModule, err := s.Pipelines[portName].AddPort(portName, port)
	if err != nil {
		log.Errorf("Error while adding port %v to pipeline", port)
		return &cni.CNIReply{}, err
	}

	log.Debugf("Rendering pipeline for port: %s", portName)
	// Render Pipeline
	if err := s.Driver.RenderModules(s.Pipelines[portName].Modules); err != nil {
		return &cni.CNIReply{Result: DriverFailure}, err
	}

	log.Debug("Updating EgressPorts for this pipeline")
	// Add all current MetaPipeline Egress ports to this port pipeline
	for _, ePort := range s.MetaPipelines[metaKey].EgressPorts {
		if err := s.connectEgressPort(ePort, s.Pipelines[portName]); err != nil {
			return &cni.CNIReply{}, err
		}
		// Add FIB entry for this port
		if err := s.updateFIB(s.Pipelines[portName], *ePort); err != nil {
			return &cni.CNIReply{}, err
		}
	}

	log.Debug("Updating EgressPorts for other pipelines")
	// Update all other Pipelines with new Egress Port, including MetaPipeline
	if err := s.updatePipelinesEgress(eModule, portName, s.Pipelines[portName].MetaKey); err != nil {
		return &cni.CNIReply{}, err
	}

	cniIfIP := &cni.CNIReply_Interface_IP{
		Version: cni.CNIReply_Interface_IP_IPV4,
		Address: port.IPAddr,
		Gateway: port.Gateway,
	}

	cniIf := &cni.CNIReply_Interface{
		Name:        portName,
		Mac:         port.MacAddr,
		IpAddresses: []*cni.CNIReply_Interface_IP{cniIfIP},
	}
	log.Debugf("CNI Reply IfIP: %+v, Interfaces: %+v", cniIfIP, cniIf)

	s.Driver.Commit()

	// Create default GW route in NS
	if port.Gateway != "" {
		log.Infof("Adding default gateway: %s for port %s, ns: %s", port.Gateway, port.PortName, port.NamesSpace)
		// container mounts to /host/proc so prepend
		realNsPath := path.Join("/host", port.NamesSpace)
		netns, err := ns.GetNS(realNsPath)
		if err != nil {
			log.Errorf("Error while opening namespace: %s: %v", realNsPath, err)
			return &cni.CNIReply{}, fmt.Errorf("failed to open netns %s: %v", realNsPath, err)
		}
		defer netns.Close()
		if err := netns.Do(func(_ ns.NetNS) error {
			link, err := netlink.LinkByName(port.IfaceName)
			if err != nil {
				return fmt.Errorf("interface name %s not found in NS", port.IfaceName)
			}
			if err := ip.AddDefaultRoute(net.ParseIP(port.Gateway), link); err != nil {
				return err
			}
			return nil
		}); err != nil {
			log.Errorf("Error while adding default gateway to NS: %v", err)
			return &cni.CNIReply{}, err
		}
	}
	return &cni.CNIReply{
		Result:     CniOk,
		Error:      "",
		Interfaces: []*cni.CNIReply_Interface{cniIf},
	}, nil
}

// Add implements CNI Add Handler.
// It returns a CNI Reply to be sent to the Nimbess CNI client.
func (s *NimbessAgent) Add(ctx context.Context, req *cni.CNIRequest) (*cni.CNIReply, error) {
	// Validate/parse CNI req
	// TODO(TROZET)

	// Hardcode to Kernel veth port for now
	// Need to make this port name unique here, so use short NS name and port name
	portName := getPortName(req)
	log.Infof("Received port add request for: %s, req: %+v", portName, req)
	port := &network.Port{
		PortName:   portName,
		Virtual:    true,
		DPDK:       false,
                UnixSocket: false,
		IfaceName:  req.GetInterfaceName(),
		NamesSpace: req.GetNetworkNamespace(),
	}

	// Protect Pipelines and driver during modification
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// Check if this port already has a pipeline
	if _, ok := s.Pipelines[portName]; ok {
		log.Error("Pod already exists, invalid CNI ADD call")
		return &cni.CNIReply{Result: CniIncompatible}, errors.New("pod already exists, invalid CNI ADD call")
	}
	// Check IPAM
	// TODO(trozet) add support for checking incoming IPAM
	var err error
	if port.IPAddr, err = invokeIPAM(InternalIPAM); err != nil {
		return &cni.CNIReply{Result: CniIncompatible}, err
	}

	// Initialize pipeline
	// Check if meta pipeline exists for this network
	var metaKey string
	if s.Config.Network.Driver == L2DriverMode {
		metaKey = req.NetworkConfig.GetName()
	} else {
		log.Error("Only L2 Network Driver mode is currently supported")
		return &cni.CNIReply{Result: CniIncompatible}, errors.New("network driver mode unsupported")
	}

	if metaKey == "" {
		log.Warning("Network Name is empty in CNI request, assuming k8s-pod-network")
		metaKey = "k8s_pod_network"
	}

	if _, ok := s.MetaPipelines[metaKey]; !ok {
		log.Infof("Meta Pipeline missing for: %s, creating.", metaKey)

		s.MetaPipelines[metaKey] = &NimbessPipeline{Name: fmt.Sprintf("%s_meta_pipeline", metaKey),
			MetaKey: metaKey, Modules: make([]network.PipelineModule, 0),
			EgressPorts: make([]*network.EgressPort, 0)}
		if err := s.MetaPipelines[metaKey].Init("", L2DriverMode, nil); err != nil {
			log.Error("Error while initializing MetaPipeline: %v", err)
			return &cni.CNIReply{Result: CniIncompatible}, err
		}

		// New metaKey in CNI Add means this must be the default network, need to add host interface if it exists
		// This interface is for pod to pod traffic and targeted to be high speed interface
		// TODO(trozet) add DPDK port on other meta network if defined, otherwise use kernel IF as slow path
		if s.Config.KernelIF != "" {
			kernIface := s.Config.KernelIF
			log.Infof("Initializing Port Pipeline for Kernel Interface %s", kernIface)
			kernPort := &network.Port{
				PortName:  s.Config.KernelIF,
				Virtual:   false,
				DPDK:      false,
                                UnixSocket:false,
				IfaceName: s.Config.KernelIF,
			}

			res, err := s.createPortPipeline(kernPort, metaKey)
			if err != nil {
				log.Errorf("Error while creating kernel interface: %v", err)
				return &cni.CNIReply{Result: CniIncompatible}, err
			}
			log.Infof("Kernel Port created: %v in meta network: %s", res.GetInterfaces(), metaKey)
		}
		// Create Kernel VPort for host networking and default routing for the pod
		log.Info("Creating Kernel Virtual Port")
		kernVPort := &network.Port{
			PortName:  "nimbess0",
			Virtual:   true,
			DPDK:      false,
                        UnixSocket:false,
			IfaceName: "nimbess0",
		}
		// Kernel VPort needs an IP, which will be the default gateway for all pods on this node
		var err error
		if kernVPort.IPAddr, err = invokeIPAM(InternalIPAM); err != nil {
			return &cni.CNIReply{Result: CniIncompatible}, err
		}
		ip, _, err := net.ParseCIDR(kernVPort.IPAddr)
		s.MetaPipelines[metaKey].Gateway = ip.String()

		res, err := s.createPortPipeline(kernVPort, metaKey)
		if err != nil {
			log.Errorf("Error while creating kernel virtual interface: %v", err)
			return &cni.CNIReply{Result: CniIncompatible}, err
		}
		log.Infof("Kernel Virtual Port created: %v in meta network: %s", res.GetInterfaces(), metaKey)
	}

	// Set gateway for port to be host VPort
	port.Gateway = s.MetaPipelines[metaKey].Gateway
	// Create Port Pipeline
	return s.createPortPipeline(port, metaKey)
}

// updateL2FIB adds a new FIB entry to a Pipeline's forwarder
func (s *NimbessAgent) updateL2FIB(l2Forwarder *network.Switch, macAddr string, gate network.Gate) error {
	if err := s.Driver.AddEntryL2FIB(l2Forwarder, macAddr, gate); err != nil {
		return err
	}
	l2Forwarder.L2FIB[macAddr] = gate
	log.Infof("Added L2FIB entry: %s:%d to switch: %s", macAddr, gate, l2Forwarder.Name)
	return nil
}

// updateFIB adds a new FIB entry to a Pipeline's forwarder for a new EgressPort
func (s *NimbessAgent) updateFIB(pipeline *NimbessPipeline, port network.EgressPort) error {
	log.Debugf("Updating L2FIB for pipeline %s, with port %s", pipeline.Name, port.GetName())
	// Check if L2 or L3
	if s.Config.Driver == L2DriverMode {
		if s.Config.MacLearn {
			log.Info("Ignoring static FIB update because MAC learning is enabled")
			return nil
		}
		// Find forwarding module in pipeline
		module := pipeline.GetModuleFromType(reflect.TypeOf(&network.Switch{}))
		if module == nil {
			log.Errorf("Unable to find Forwarding Module in pipeline: %s", pipeline.Name)
			return errors.New("unable to find Forwarding Module in Pipeline")
		}

		// If L2 get MAC and update FIB with forwarder outgoing gate to EgressPort
		// TODO this means that the forwarder must be directly connected to EgressPort,
		// need to look at the logic for if this isn't the case
		for gate, mod := range module.GetEGateMap() {
			log.Debugf("L2FIB Gate search. Gate: %d, mod: %s, port: %s", gate, mod.GetName(), port.Name)
			if mod.GetName() == port.GetName() {
				log.Debugf("Gate %d found for port %v", gate, port)
				return s.updateL2FIB(module.(*network.Switch), port.MacAddr, gate)
			}
		}
		log.Errorf("Unable to find gate during L2FIB update for EgressPort %s on Switch %s",
			port.Name, module.GetName())
		return errors.New("unable to find gate for EgressPort on Switch")

	}
	return errors.New("unsupported driver type for FIB update specified")
}

// connectEgressPort connects an Egress port to a Nimbess Port Pipeline
// Also will trigger an update to forwarder FIB if required.
func (s *NimbessAgent) connectEgressPort(port *network.EgressPort, pipeline *NimbessPipeline) error {
	log.Infof("Adding Egress Port %s to pipeline %s", port.Name, pipeline.Name)
	// Get last module of pipeline that is not port
	lastMod := pipeline.GetLastModule([]reflect.Type{reflect.TypeOf(&network.EgressPort{})})
	log.Debugf("Last module found as %s, for pipeline %v", lastMod, *pipeline)
	// TODO re-examine this for proper gate selection
	// Connect port to last module
	port.Connect(lastMod, false, nil)
	// Connect last module to port
	lastMod.Connect(port, true, nil)
	pipeline.EgressPorts = append(pipeline.EgressPorts, port)
	if err := s.Driver.RenderModules([]network.PipelineModule{port}); err != nil {
		log.Error("Failed to Add Egress port to pipeline")
		return err
	}
	log.Infof("EgressPort %s connected to Pipeline %s", port.Name, pipeline.Name)
	return nil
}

// updatePipelinesEgress adds a new egress port to all Port pipelines that do not contain
// excludedKey, as long as they belong to same network (metaKey).
// Also will trigger an update to forwarder FIB if required.
func (s *NimbessAgent) updatePipelinesEgress(port *network.EgressPort, excludedKey string, metaKey string) error {
	log.Infof("Updating pipelines with new egress port: %s", port.Name)
	// Add Port to MetaPipeline
	s.MetaPipelines[metaKey].EgressPorts = append(s.MetaPipelines[metaKey].EgressPorts, port)
	// Search port pipelines to add port to
	for k, p := range s.Pipelines {
		if k != excludedKey && p.MetaKey == metaKey {
			if err := s.connectEgressPort(port, p); err != nil {
				return err
			}
			if err := s.updateFIB(p, *port); err != nil {
				return err
			}
		}
	}
	log.Info("Completed Egress pipeline update")
	return nil
}

// removeEgressPort removes an Egress Port from a Meta Pipeline by name
func (s *NimbessAgent) removeEgressPort(name string, metaKey string) error {
	log.Debugf("Removing Egress Port: %s", name)
	for idx, ePort := range s.MetaPipelines[metaKey].EgressPorts {
		if ePort.GetName() == name {
			log.Debugf("Found egress port for removal from Meta Pipeline: %s", name)
			s.MetaPipelines[metaKey].EgressPorts = append(s.MetaPipelines[metaKey].EgressPorts[:idx],
				s.MetaPipelines[metaKey].EgressPorts[idx+1:]...)
			log.Debugf("Egress Ports in Metapipeline: %v", s.MetaPipelines[metaKey].EgressPorts)
			return nil
		}
	}
	log.Error("Failed to find Egress Port in MetaPipeline: %s", metaKey)
	return fmt.Errorf("unable to find egress port %s to remove in metapipeline with key: %s", name, metaKey)
}

// Delete implements CNI Delete Handler.
// It returns a CNI Reply to be sent to the Nimbess CNI client.
func (s *NimbessAgent) Delete(ctx context.Context, req *cni.CNIRequest) (*cni.CNIReply, error) {
	// If  network NS is missing, we should check for previous result
	// For now we will leave that as TODO
	// If NS is missing just return OK
	if req.NetworkNamespace == "" {
		log.Debugf("Delete received with no namespace: %+v", req)
		return &cni.CNIReply{Result: CniOk}, nil
	}
	portName := getPortName(req)
	log.Infof("Received port del request for: %s", portName)

	// Protect Pipelines and driver during modification
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// Check if this port already has a pipeline
	if _, ok := s.Pipelines[portName]; !ok {
		log.Infof("Port %s has no pipeline, ignoring request", portName)
		return &cni.CNIReply{Result: CniOk}, nil
	}

	// Delete all modules in the port pipeline except for egress ports
	log.Infof("Deleting pipeline for %s", portName)
	if err := s.Driver.DeleteModules(s.Pipelines[portName].Modules, false); err != nil {
		return &cni.CNIReply{Result: DriverFailure}, err
	}

	log.Infof("Deleting egress ports for port %s", portName)
	// Delete egress port
	ePort := &network.EgressPort{Module: network.Module{Name: fmt.Sprintf("%s_egress", portName)},
		Port: &network.Port{}}
	var modules []network.PipelineModule
	modules = append(modules, ePort)
	if err := s.Driver.DeleteModules(modules, true); err != nil {
		return &cni.CNIReply{Result: DriverFailure}, err
	}
	// Need to disconnect all other egress ports from pipeline
	if err := s.Pipelines[portName].DisconnectPipeline(reflect.TypeOf(&network.EgressPort{})); err != nil {
		return &cni.CNIReply{Result: DriverFailure}, err
	}

	// Remove egress port from MetaPipeline Egress Ports slice
	if err := s.removeEgressPort(fmt.Sprintf("%s_egress", portName), s.Pipelines[portName].MetaKey); err != nil {
		return &cni.CNIReply{Result: DriverFailure}, err
	}

	s.Driver.Commit()

	log.Infof("Delete for req is successful: %+v", req)
	delete(s.Pipelines, portName)
	return &cni.CNIReply{Result: CniOk}, nil
}

// Run starts up the main Agent daemon.
func (s *NimbessAgent) Run() error {
	log.Info("Starting Nimbess Agent...")
	log.Info("Connecting to Data Plane")
	dpConn := s.Driver.Connect()
	defer dpConn.Close()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Config.Port))
	if err != nil {
		log.Errorf("Failed to listen on port: %d", s.Config.Port)
		return err
	}
	log.Info("Starting Nimbess gRPC server...")
	grpcServer := grpc.NewServer()
	cni.RegisterRemoteCNIServer(grpcServer, s)
	err = grpcServer.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}
