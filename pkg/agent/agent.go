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
	"github.com/containernetworking/cni/pkg/types/current"
	"github.com/containernetworking/plugins/pkg/ip"
	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/denisbrodbeck/machineid"
	"github.com/intel/userspace-cni-network-plugin/usrsptypes"
	"github.com/nimbess/nimbess-agent/pkg/agent/config"
	"github.com/nimbess/nimbess-agent/pkg/etcdv3"
	"github.com/nimbess/nimbess-agent/pkg/etcdv3/model"
	"github.com/nimbess/nimbess-agent/pkg/network"
	"github.com/nimbess/nimbess-agent/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/vishvananda/netlink"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	v1_types "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"net"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/nimbess/nimbess-agent/pkg/drivers"
	"github.com/nimbess/nimbess-agent/pkg/proto/cni"
	"github.com/nimbess/nimbess-agent/pkg/userspace"
)

// CNI Reply Values
const (
	CniOk                = 0
	CniIncompatible      = 1
	InvalidNetworkConfig = 2
	ContainerNotExist    = 3
	CniTryLater          = 11
	DriverFailure        = 101
	FastPathFailure      = 102
	InternalIPAM         = "nimbess"
	FastPathNetwork      = "fastpath"
	DefaultBaseCNIDir    = "/var/lib/nimbess/cni"
	DefaultPodNs         = "default"
)

// For internal Nimbess IPAM
// TODO(trozet): change this to IPv6
var ipAddr = net.IP{40, 0, 0, 0}
var ipAddrFast = net.IP{50, 0, 0, 0}

// Used to track BESS vhost PMD Ports
var vhostIndex = 0

type L2FIBEntry struct {
	permanent bool
	age       int64
	port      string
}

// NimbessAgent represents the agent runtime server.
// It contains a loadable runtime data plane driver to manage the data plane.
// It includes a mutex used to handle locking between driver and agent events to force a
// single-processed event pipeline.

type NimbessAgent struct {
	ID          string
	EtcdContext context.Context
	Mu          *sync.Mutex
	Config      *config.NimbessConfig
	Driver      drivers.Driver
	KubeClient  kubernetes.Interface
	// Pipelines contains a map of port name to pipeline pointer
	Pipelines map[string]*NimbessPipeline
	// MetaPipelines contains a map of network name to abstract pipelines
	MetaPipelines map[string]*NimbessPipeline
	notifications chan network.L2FIBCommand
	EtcdClient    etcdv3.Client
}

// getPortName translates a CNI request into a Nimbess Port Identifier
func getPortName(req *cni.CNIRequest) string {
	return fmt.Sprintf("port_%s_%s", req.ContainerId, req.InterfaceName)
}

// invokes IPAM either internal or external and returns the CIDR
func invokeIPAM(ipamType string, fastPath bool) (string, error) {
	//TODO(trozet): Add support for external IPAM
	if ipamType == InternalIPAM {
		if fastPath {
			ipAddrFast[3]++
			return fmt.Sprintf("%s/24", ipAddrFast.String()), nil
		}
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
	if err := s.Pipelines[portName].Init(portName, config.L2DriverMode, s.MetaPipelines[metaKey]); err != nil {
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

	// TODO(trozet): There may be a potential race condition where driver commit is called and this function
	// returns before the objects actually exist in the data plane. The issue is that a CNI DEL may immediately
	// follow the CNI ADD, and the objects may not exist yet in the data plane. May need to add a check in the commit
	// that the objects exist in the data plane before returning from it.
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
	log.Debug("CNI ADD complete")
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

	// Create Kernel veth port for default network
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
	if port.IPAddr, err = invokeIPAM(InternalIPAM, false); err != nil {
		return &cni.CNIReply{Result: CniIncompatible}, err
	}

	// Initialize pipeline
	// Check if meta pipeline exists for this network
	var metaKey string
	if s.Config.Network.Driver == config.L2DriverMode {
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
		if err := s.MetaPipelines[metaKey].Init("", config.L2DriverMode, nil); err != nil {
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
				PortName:   s.Config.KernelIF,
				Virtual:    false,
				DPDK:       false,
				UnixSocket: false,
				IfaceName:  s.Config.KernelIF,
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
			PortName:   "nimbess0",
			Virtual:    true,
			DPDK:       false,
			UnixSocket: false,
			IfaceName:  "nimbess0",
		}
		// Kernel VPort needs an IP, which will be the default gateway for all pods on this node
		var err error
		if kernVPort.IPAddr, err = invokeIPAM(InternalIPAM, false); err != nil {
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

	// Create FastPath Network
	// TODO(trozet): In the future remove automatically creating a pipeline/network for the port. We will rely on
	// UNP to pass the configuration for how the port should be used and at CNI add time only create the Port itself
	fastMetaKey := fmt.Sprintf("%s-%s", metaKey, FastPathNetwork)
	if _, ok := s.MetaPipelines[fastMetaKey]; !ok {
		log.Infof("Meta Pipeline missing for: %s, creating.", fastMetaKey)

		s.MetaPipelines[fastMetaKey] = &NimbessPipeline{Name: fmt.Sprintf("%s_meta_pipeline", fastMetaKey),
			MetaKey: fastMetaKey, Modules: make([]network.PipelineModule, 0),
			EgressPorts: make([]*network.EgressPort, 0)}
		if err := s.MetaPipelines[fastMetaKey].Init("", config.L2DriverMode, nil); err != nil {
			log.Error("Error while initializing MetaPipeline: %v", err)
			return &cni.CNIReply{Result: CniIncompatible}, err
		}

		// Right now we only support DPDK fast path, but in the future make this configurable for alternate
		// FastPaths like XDPa

		// Check if physical port defined in config, if present and no meta pipeline we need to create
		// this port MUST be bound to DPDK already, along with a data plane restart
		if s.Config.FastPathIF != "" {
			if err := util.ValidatePCI(s.Config.FastPathIF); err != nil {
				log.Error("error validating PCI address: %s", err)
				return &cni.CNIReply{Result: FastPathFailure}, err
			}

			fastPhyName := "dpdk0"
			fastPhyIP, err := invokeIPAM(InternalIPAM, true)
			if err != nil {
				return &cni.CNIReply{Result: CniIncompatible}, err
			}

			fastPhy := &network.Port{
				PortName:  fastPhyName,
				Virtual:   false,
				DPDK:      true,
				IPAddr:    fastPhyIP,
				IfaceName: s.Config.FastPathIF,
			}

			res, err := s.createPortPipeline(fastPhy, fastMetaKey)
			if err != nil {
				log.Errorf("Error while creating FastPath physical interface: %v", err)
				return &cni.CNIReply{Result: FastPathFailure}, err
			}
			log.Infof("FastPath Physical Port created: %v in meta network: %s", res.GetInterfaces(), fastMetaKey)
		}
	}

	// Create virtual port for POD
	fastPortName := fmt.Sprintf("%s-%s", portName, FastPathNetwork)
	fastPortIP, err := invokeIPAM(InternalIPAM, true)
	if err != nil {
		return &cni.CNIReply{Result: CniIncompatible}, err
	}
	fastPort := &network.Port{
		PortName:   fastPortName,
		Virtual:    true,
		DPDK:       true,
		IPAddr:     fastPortIP,
		IfaceName:  fmt.Sprintf("eth_vhost%d", vhostIndex),
		SocketPath: fmt.Sprintf("%s/%s_vhost.sock", DefaultBaseCNIDir, req.ContainerId),
	}
	vhostIndex++
	res, err := s.createPortPipeline(fastPort, fastMetaKey)
	if err != nil {
		log.Errorf("Error while creating FastPath virtual interface: %v", err)
		return &cni.CNIReply{Result: FastPathFailure}, err
	}
	log.Infof("FastPath Virtual Port created: %v in meta network: %s", res.GetInterfaces(), fastMetaKey)

	// Create configuration data to prepare for annotation
	fastIP, fastNet, err := net.ParseCIDR(fastPort.IPAddr)
	if err != nil {
		return &cni.CNIReply{Result: FastPathFailure}, fmt.Errorf("failed to parse FP IP: %s", fastPort.IPAddr)
	}
	ipConf := &current.IPConfig{
		Address: net.IPNet{IP: fastIP, Mask: fastNet.Mask},
	}
	ifConf := &current.Interface{
		Name: fastPort.IfaceName,
		Mac:  fastPort.MacAddr,
	}
	ipRes := current.Result{
		IPs:        []*current.IPConfig{ipConf},
		Interfaces: []*current.Interface{ifConf},
	}
	vhostConf := usrsptypes.VhostConf{
		Mode:       "client",
		Socketfile: fastPort.SocketPath,
	}
	usrConf := usrsptypes.UserSpaceConf{
		IfType:    "vhostuser",
		VhostConf: vhostConf,
	}
	configData := &usrsptypes.ConfigurationData{
		ContainerId: req.ContainerId,
		IfName:      fastPort.IfaceName,
		Config:      usrConf,
		IPResult:    ipRes,
	}
	log.Debugf("Annotation data set as: %+v", configData)
	if err := annotatePod(s.KubeClient, req, configData); err != nil {
		log.Errorf("Failed to annotate Pod: %v", err)
		return &cni.CNIReply{Result: FastPathFailure}, err
	}

	// Set gateway for port to be host VPort
	port.Gateway = s.MetaPipelines[metaKey].Gateway
	// Create kernel Port Pipeline
	return s.createPortPipeline(port, metaKey)
}

func (s *NimbessAgent) updateFIBFromEgress(pipeline *NimbessPipeline) error {
	egressPorts := pipeline.GetEgressPorts()
	if len(egressPorts) == 0 {
		log.Debugf("No egress ports detected, skipping FIB update")
		return nil
	}

	for _, ePort := range egressPorts {
		if err := s.updateFIB(pipeline, *ePort); err != nil {
			return err
		}
	}
	return nil
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

func (s *NimbessAgent) addMACtoL2FIB(MAC string, pipeline *NimbessPipeline, portname string) error {
	log.Debugf("Adding MAC for pipeline %s, with port %s", pipeline.Name, portname)

	l2switch := pipeline.GetModuleFromType(reflect.TypeOf(&network.Switch{}))
	if l2switch == nil {
		log.Errorf("Unable to find Forwarding Module in pipeline: %s", pipeline.Name)
		return errors.New("unable to find Forwarding Module in Pipeline")
	}
	for gate, mod := range l2switch.GetEGateMap() {
		if mod == nil {
			continue
		}
		log.Debugf("L2FIB Gate search. Gate: %d, mod: %s, port: %s", gate, mod.GetName(), portname)
		if mod.GetName() == portname {
			log.Debugf("Gate %d found for port %v", gate, portname)
			return s.updateL2FIB(l2switch.(*network.Switch), MAC, gate)
		}
	}
	return errors.New("unable to find gate for EgressPort on Switch")
}

func (s *NimbessAgent) delMACfromFIB(MAC string, pipeline *NimbessPipeline) error {
	log.Debugf("Deleting MAC for pipeline %s", pipeline.Name)

	l2switch := pipeline.GetModuleFromType(reflect.TypeOf(&network.Switch{}))
	if l2switch == nil {
		log.Errorf("Unable to find Forwarding Module in pipeline: %s", pipeline.Name)
		return errors.New("unable to find Forwarding Module in Pipeline")
	}
	return s.Driver.DelEntryL2FIB(l2switch.(*network.Switch), MAC)
}

// updateFIB adds a new FIB entry to a Pipeline's forwarder for a new EgressPort
func (s *NimbessAgent) updateFIB(pipeline *NimbessPipeline, port network.EgressPort) error {
	log.Debugf("Updating L2FIB for pipeline %s, with port %s", pipeline.Name, port.GetName())
	// Check if L2 or L3
	if s.Config.Driver == config.L2DriverMode {
		// Find forwarding module in pipeline
		module := pipeline.GetModuleFromType(reflect.TypeOf(&network.Switch{}))
		if module == nil {
			log.Errorf("Unable to find Forwarding Module in pipeline: %s", pipeline.Name)
			return errors.New("unable to find Forwarding Module in Pipeline")
		}
		pipeline.l2fib[port.MacAddr] = &L2FIBEntry{
			permanent: true,
			age:       0,
			port:      port.GetName(),
		}
		// If L2 get MAC and update FIB with forwarder outgoing gate to EgressPort
		// TODO this means that the forwarder must be directly connected to EgressPort,
		// need to look at the logic for if this isn't the case
		for gate, mod := range module.GetEGateMap() {
			if mod == nil {
				continue
			}
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
	pipeline.Modules = append(pipeline.Modules, port)
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
	log.Warnf("Failed to find Egress Port in MetaPipeline: %s", metaKey)
	return nil
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
	reqPortName := getPortName(req)
	log.Infof("Received port del request for: %s", reqPortName)

	// Protect Pipelines and driver during modification
	s.Mu.Lock()
	defer s.Mu.Unlock()

	entriesToDelete := make([]string, 0)
	if pipeline, ok := s.Pipelines[reqPortName]; ok {
		for MAC, entry := range pipeline.l2fib {
			if entry.port == reqPortName {
				entriesToDelete = append(entriesToDelete, MAC)
			}
		}
		for _, MAC := range entriesToDelete {
			delete(pipeline.l2fib, MAC)
		}
	}

	for _, portName := range []string{reqPortName, fmt.Sprintf("%s-%s", reqPortName, FastPathNetwork)} {
		// Check if this port already has a pipeline
		if _, ok := s.Pipelines[portName]; !ok {
			log.Infof("Port %s has no pipeline, ignoring request", portName)
			continue
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

		// Finally delete port
		if err := s.Driver.DeletePort(portName); err != nil {
			return &cni.CNIReply{Result: DriverFailure}, err
		}

		s.Driver.Commit()

		log.Infof("Delete for req is successful: %+v", req)
		delete(s.Pipelines, portName)
	}
	return &cni.CNIReply{Result: CniOk}, nil
}

func annotatePod(k8sClient kubernetes.Interface, req *cni.CNIRequest, configData *usrsptypes.ConfigurationData) error {
	log.Info("Writing pod annotation")
	podName := req.GetPodName()
	podNamespace := req.GetPodNamespace()
	if podNamespace == "" {
		podNamespace = DefaultPodNs
	}
	var pod *v1_types.Pod
	// If we were not given pod name from CNI, we cannot continue
	if podName == "" {
		return errors.New("pod name is empty")
	}
	var pErr error
	if pod, pErr = k8sClient.CoreV1().Pods(podNamespace).Get(podName, v1.GetOptions{}); pErr != nil {
		return fmt.Errorf("unable to query pod %s, error: %v", podName, pErr)
	}
	log.Debugf("Pod found: %+v", pod)
	if pod == nil {
		return fmt.Errorf("unable to find pod for pod name: %s", podName)
	}

	var modifiedConfig, modifiedMappedDir bool
	var err error
	// TODO(trozet): work with userspace CNI to remove unused args and k8s custom client
	modifiedConfig, err = userspace.SetPodAnnotationConfigData(pod, configData)
	if err != nil {
		return fmt.Errorf("error formatting annotation configData: %v", err)
	}
	// Retrieve the mappedSharedDir from the Containers in podSpec. Directory
	// in container Socket Files will be read from. Write this data back as an
	// annotation so container knows where directory is located.
	mappedSharedDir, err := userspace.GetPodVolumeMountHostMappedSharedDir(pod)
	if err != nil {
		log.Warnf("Cannot find mapped shared dir: %v,  defaulting to: %s", err, DefaultBaseCNIDir)
		mappedSharedDir = DefaultBaseCNIDir
	}
	modifiedMappedDir, err = userspace.SetPodAnnotationMappedDir(pod, mappedSharedDir)
	if err != nil {
		return fmt.Errorf("error setting annotation for mappedSharedDir - %v", err)
	}

	if modifiedConfig == true || modifiedMappedDir == true {
		// Update the pod
		pod = pod.DeepCopy()
		if resultErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			if err != nil {
				// Re-get the pod unless it's the first attempt to update
				pod, err = k8sClient.CoreV1().Pods("").Get(podName, v1.GetOptions{})
				if err != nil {
					return err
				}
			}

			pod, err = k8sClient.CoreV1().Pods(pod.Namespace).UpdateStatus(pod)
			return err
		}); resultErr != nil {
			return fmt.Errorf("update failed for pod %s/%s: %v", pod.Namespace, pod.Name, resultErr)
		}
	}
	log.Debugf("Pod annotation complete: %s", pod.Name)
	return nil
}

func (s *NimbessAgent) processNotification() bool {

	fibRequest := <-s.notifications
	log.Debugf("Processing FIB Request %s MAC %s", fibRequest.Command, fibRequest.MAC)

	s.Mu.Lock()
	defer s.Mu.Unlock()

	// pipeline's fib points to its parent metapipeline fib
	pipeline, ok := s.Pipelines[fibRequest.Port]

	if !ok {
		log.Errorf("Failed to find metaPipeline and its fib for port: %s", fibRequest.Port)
		return true /* This is bad, but it is not a reason to exit the notification thread */
	}

	if fibRequest.Command == "LEARN" {
		// Learned
		entry, ok := pipeline.l2fib[fibRequest.MAC]
		if !ok {
			pipeline.l2fib[fibRequest.MAC] = &L2FIBEntry{
				permanent: false,
				age:       fibRequest.Setage,
				port:      fibRequest.Port,
			}
			for key, targetPipeline := range s.Pipelines {
				if key != fibRequest.Port {
					// no Routing for self
					s.addMACtoL2FIB(fibRequest.MAC, targetPipeline, fmt.Sprintf("%s_egress", fibRequest.Port))
				}
			}
		} else {
			if !entry.permanent {
				entry.age = time.Now().Unix()
			}
		}
	}
	if fibRequest.Command == "EXPIRE" {
		entry, ok := pipeline.l2fib[fibRequest.MAC]
		if ok && (!entry.permanent) {
			delete(pipeline.l2fib, fibRequest.MAC)
			for _, targetPipeline := range s.Pipelines {
				s.delMACfromFIB(fibRequest.MAC, targetPipeline)
			}
		}
	}
	if fibRequest.Command == "ADD" {
		pipeline.l2fib[fibRequest.MAC] = &L2FIBEntry{
			permanent: true,
			age:       0,
			port:      fibRequest.Port,
		}
		for key, targetPipeline := range s.Pipelines {
			if key != fibRequest.Port {
				s.addMACtoL2FIB(fibRequest.MAC, targetPipeline, fibRequest.Port)
			}
		}
	}
	return true //Deal with control later
}

func (s *NimbessAgent) runNotifications() {
	log.Debugf("Started Notification thread")
	for s.processNotification() {
	}
	log.Debugf("Notification thread exited")
}

// Init initializes the agent
func (s *NimbessAgent) Init() error {
	firstStart := true
	// Find agent ID
	id, err := machineid.ID()
	if err != nil {
		return err
	} else if id == "" {
		return errors.New("unable to find machine ID")
	}

	log.Infof("Agent ID: %s", id)
	s.ID = id
	k := model.AgentKey{
		MachineID: id,
	}
	kv := &model.KVPair{Key: k, Value: model.Agent{LastStart: time.Now().String()}}
	// check for ID in etcd, if exists we know this is not first time startup
	err = s.EtcdClient.Create(s.EtcdContext, kv)
	if err != nil {
		if cerr, ok := err.(*etcdv3.StorageError); ok {
			if cerr.Code == etcdv3.ErrCodeKeyExists {
				firstStart = false
				log.Info("Previous agent data detected in database")
			} else {
				return err
			}
		} else {
			return err
		}
	}

	// TODO(trozet) handle resync if not first start of data plane
	// we want to do this before Agent Runs to and accepts CNI requests
	log.Infof("Agent has finished initialization, first time startup: %t", firstStart)
	return nil
}

// EnsureNetwork ensures that a network exists already in meta pipelines and if not, creates it from
// a network attachment definition. If the network attachment definition does not exist, an error is thrown.
// boolean returned is true if this network already existed in meta pipelines, false if it was initialized here
func (s *NimbessAgent) EnsureNetwork(network string) (bool, error) {
	if _, ok := s.MetaPipelines[network]; ok {
		return true, nil
	}
	// TODO(trozet): Need to do network attachment CRD lookup for network
	// for now just return error if network doesn't already exist
	return false, fmt.Errorf("failed to ensure network exists: %s", network)
}

// Run starts up the main Agent daemon.
func (s *NimbessAgent) Run() error {
	log.Info("Starting Nimbess Agent...")
	defer s.EtcdClient.Close()
	log.Info("Connecting to Data Plane")
	dpConn := s.Driver.Connect()
	s.notifications = s.Driver.GetNotifications()
	go s.runNotifications()
	defer dpConn.Close()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Config.Port))
	if err != nil {
		log.Errorf("Failed to listen on port: %d", s.Config.Port)
		return err
	}

	// Create watchers and start handler for Stargazer models
	if err = s.runGazers(); err != nil {
		log.Errorf("Error while running gazers")
		return err
	}

	// Start main server (blocking)
	log.Info("Starting Nimbess gRPC server...")
	grpcServer := grpc.NewServer()
	cni.RegisterRemoteCNIServer(grpcServer, s)
	err = grpcServer.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}
