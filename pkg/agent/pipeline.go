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

// Network Pipeline implementation for Nimbess Agent

package agent

import (
	"fmt"
	"github.com/mohae/deepcopy"
	"reflect"
	"sync"

	"github.com/nimbess/nimbess-agent/pkg/drivers"
	"github.com/nimbess/nimbess-agent/pkg/network"

	log "github.com/sirupsen/logrus"
)

// NimbessPipeline implements a Pipeline of network functions for packet traversal
type NimbessPipeline struct {
	Name        string
	Mu          *sync.Mutex
	Modules     []network.PipelineModule
	Driver      drivers.Driver
	EgressPorts []*network.EgressPort
	MetaKey     string
	Gateway     string
}

// GetModule returns the module
func (s *NimbessPipeline) GetModule(name string) network.PipelineModule {
	for _, mod := range s.Modules {

		if mod.GetName() == name {
			return mod
		}
	}

	return nil
}

// AddPort adds a port to a pipeline.
// It returns a pointer to a EgressPort module to update other pipelines with
func (s *NimbessPipeline) AddPort(name string, port *network.Port) (*network.EgressPort, error) {
	log.Infof("Adding new port %v to pipeline: %v", port, s.Name)
	portMod := network.Module{Name: fmt.Sprintf("%s_ingress", name),
		EGates: network.MakeGateMap()}
	// Create Ingress Port
	iPort := &network.IngressPort{Port: port, Module: portMod}
	s.Modules[0] = iPort
	// Set forwarder linkage
	// Module at index 1 should always be first meta module and already be set
	gate := iPort.Connect(s.Modules[1], true, nil)
	log.Debugf("Port %s via gate: %d connected to module %s", iPort.Name, gate, s.Modules[1].GetName())

	// Update forwarder module with reverse linkage
	gate = s.Modules[1].Connect(iPort, false, nil)
	log.Debugf("Module %s via gate: %d connected to port %s", s.Modules[1].GetName(), gate, iPort.Name)

	// Create Egress port
	ePortMod := network.Module{Name: fmt.Sprintf("%s_egress", name),
		IGates: network.MakeGateMap()}
	ePort := &network.EgressPort{Port: port, Module: ePortMod}
	return ePort, nil
}

func getForwarderModule(forwarder string, metaKey string) (network.PipelineModule, error) {
	switch forwarder {
	case L2DriverMode:
		mod := network.Module{Name: fmt.Sprintf("Switch_%s", metaKey),
			IGates: network.MakeGateMap(),
			EGates: network.MakeGateMap(),
		}
		sMod := &network.Switch{Module: mod, L2FIB: make(map[string]network.Gate)}
		return sMod, nil
	default:
		return nil, fmt.Errorf("invalid forwarder given: %s", forwarder)
	}
}

// Init is responsible for initializing a new pipeline.
// It checks the abstract pipeline for a network and builds the pipeline
// for this port. If meta pipeline is not nil, s is a port pipeline.
func (s *NimbessPipeline) Init(port string, forwarder string, metaPipeline *NimbessPipeline) error {
	if metaPipeline != nil {
		log.Infof("Initializing pipeline for port %s", port)
		if len(metaPipeline.Modules) == 0 {
			return fmt.Errorf("empty Module list for metapipeline: %v", metaPipeline)
		}
		// first index in port pipeline slice is always ingress port, reserve it
		s.Modules = append(s.Modules, nil)
		// Need to do deep copy Meta pipeline for port, and make unique module names
		// Update linked list of modules by making new modules from meta pipeline for this pipeline
		s.createPipeline(metaPipeline.Modules[0], port)
	} else {
		if len(s.Modules) > 0 {
			log.Debugf("Pipeline already exists, ignoring Init on meta pipeline: %v", metaPipeline)
			return nil
		}
		// Need to create meta pipeline from scratch, just include forwarder module
		mod, err := getForwarderModule(forwarder, s.MetaKey)
		if err != nil {
			return err
		}
		// Set meta module name to be itself, just in case it gets copied by port pipeline module
		mod.SetMeta(mod.GetName())
		s.Modules = append(s.Modules, mod)
		log.Debugf("Added forwarder module to meta pipeline: %s", mod.GetName())
	}

	return nil
}

// createPipeline recursively creates modules and then updates its linkage from meta pipeline module
// module should be the a pointer to the first module in a meta pipeline
// port is name of new port used for this new port pipeline
func (s *NimbessPipeline) createPipeline(module network.PipelineModule, port string) network.PipelineModule {
	log.Debugf("Entered Linked List Create for module: %+v", module)
	var newMod network.PipelineModule
	// Check to see if the module was already added to this port pipeline
	for _, mod := range s.Modules {
		if mod == nil {
			continue
		}
		if mod.GetMeta() == module.GetName() {
			newMod = mod
			break
		}
	}
	// If module doesn't exist we need to create a copy
	if newMod == nil {
		log.Debugf("Creating copy of meta module for Linked list")
		newMod := deepcopy.Copy(module)
		// Unique module names for pipeline use format <Module name>_<port name>
		newName := fmt.Sprintf("%s_%s", newMod.(network.PipelineModule).GetName(), port)
		// We can assert Module safely here because every complex module should always inherit Module
		newMod.(network.PipelineModule).SetName(newName)
		// Create new maps for gates, as new mappings will be built recursively
		newMod.(network.PipelineModule).SetEGateMap(network.MakeGateMap())
		newMod.(network.PipelineModule).SetIGateMap(network.MakeGateMap())
		// Append new module to existing module list
		s.Modules = append(s.Modules, newMod.(network.PipelineModule))
		log.Debugf("Module created: %+v", newMod)
	}

	// Walk meta module Ingress gates and connect new port pipeline modules
	for gate, mod := range module.GetIGateMap() {
		if mod != nil {
            newMod.Connect(s.createPipeline(mod, port), false, &gate)
		}
	}

	// Walk meta module Egress gates and connect new port pipeline modules
	for gate, mod := range module.GetEGateMap() {
		if mod != nil {
            newMod.Connect(s.createPipeline(mod, port), true, &gate)
		}
	}
	return newMod
}

// GetLastModule returns a pointer to the last module in a pipeline.
// excludedTypes may be used to exclude modules of certain types (like EgressPort).
func (s *NimbessPipeline) GetLastModule(excludedTypes []reflect.Type) network.PipelineModule {
	for i := len(s.Modules) - 1; i >= 0; i-- {
		found := true
		for _, excludedType := range excludedTypes {
			if reflect.TypeOf(s.Modules[i]) == excludedType {
				found = false
				break
			}
		}

		if found {
			return s.Modules[i]
		}

	}

	return nil
}

// DisconnectPipeline disconnects any modules of type modType that are attached to the last module in a pipeline
func (s *NimbessPipeline) DisconnectPipeline(modType reflect.Type) error {
	log.Debugf("Searching for modules to disconnect from pipeline %s of type %s", s.Name, modType.Name())
	lastMod := s.GetLastModule([]reflect.Type{modType})
	if lastMod == nil {
		return fmt.Errorf("could not find last module in pipeline %s for disconnect", s.Name)
	}
	for egate, mod := range lastMod.GetEGateMap() {
		// search gates in connecting module to remove
		for igate, iMod := range mod.GetIGateMap() {
			if iMod == lastMod {
				log.Debugf("Disconnecting gate %d on module %s, which points to %s", igate, mod.GetName(),
					iMod.GetName())
				mod.Disconnect(igate, false)
				break
			}
		}
		lastMod.Disconnect(egate, true)
	}
	return nil
}

// GetModuleFromType gets the first Module found of a specific type
func (s *NimbessPipeline) GetModuleFromType(modType reflect.Type) network.PipelineModule {
	for i := 0; i < len(s.Modules); i++ {
		if reflect.TypeOf(s.Modules[i]) == modType {
			return s.Modules[i]
		}
	}
	log.Debugf("No Module of type: %v, found in Pipeline: %s, modules: %v", modType, s.Name, s.Modules)
	return nil
}
