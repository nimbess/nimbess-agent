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
	"reflect"
	"sync"

	"github.com/nimbess/nimbess-agent/pkg/agent/config"
	"github.com/nimbess/nimbess-agent/pkg/drivers"
	"github.com/nimbess/nimbess-agent/pkg/network"
	"github.com/nimbess/nimbess-agent/pkg/util"

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
	l2fib       map[string]*L2FIBEntry
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
	case config.L2DriverMode:
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
		// fixup the copy of the l2fib created by createPipeline and replace it with a ref
		s.l2fib = metaPipeline.l2fib
	} else {
		if len(s.Modules) > 0 {
			log.Debugf("Pipeline already exists, ignoring Init on meta pipeline: %v", metaPipeline)
			return nil
		}
		// Need to create meta pipeline from scratch, just include forwarder module
		mod, err := getForwarderModule(forwarder, s.MetaKey)
		s.l2fib = make(map[string]*L2FIBEntry)
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
		// Append new module to existing module list
		newMod = createModFromMeta(module, port)
		s.Modules = append(s.Modules, newMod)
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
	if modType == nil {
		return nil
	}
	log.Debugf("Searching for modules to disconnect from pipeline %s of type %s", s.Name, modType.Name())
	lastMod := s.GetLastModule([]reflect.Type{modType})
	if lastMod == nil {
		return fmt.Errorf("could not find last module in pipeline %s for disconnect", s.Name)
	}
	for egate, mod := range lastMod.GetEGateMap() {
		if mod == nil {
			continue
		}
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

// Inserts a Module at position idx in a pipeline. If idx is 0, the module is inserted after the source port
// If index is less than 0, it is inserted at the end of pipeline before all egress ports
// agent is a pointer to the NimbessAgent to use to render the changes when a port pipeline is updated
func (s *NimbessPipeline) InsertModule(mod network.PipelineModule, index int, agent *NimbessAgent) error {
	log.Debugf("Inserting module into pipeline %s at idx %d, %+v", s.Name, index, mod)
	// startIdx is used to determined the index of the first non-port module (aka the start of the meta pipeline)
	startIdx := 0
	isPortPipeline := false
	// Find first non source port element to find real beginning index
	for idx, module := range s.Modules {
		if reflect.TypeOf(module) != reflect.TypeOf(&network.IngressPort{}) {
			startIdx = idx
			break
		}
	}

	// Find last non egress port element to find real end index
	endIdx := len(s.Modules) - 1
	for i := endIdx; i >= 0; i-- {
		if reflect.TypeOf(s.Modules[i]) != reflect.TypeOf(network.EgressPort{}) {
			endIdx = i
			break
		}
	}

	if startIdx != 0 {
		log.Debugf("Inserting into Port Pipeline. Beginning idx: %d, End idx: %d", startIdx, endIdx)
		isPortPipeline = true
	}

	// This is a requested insert at beginning of a meta or port pipeline
	if index == 0 {
		// Update Modules slice
		s.Modules = append(s.Modules, mod)
		copy(s.Modules[startIdx+1:], s.Modules[startIdx:])
		s.Modules[startIdx] = mod

		// If startIdx does not equal 0, then there must be a previous port we are connecting to
		// wire it
		// Need to find previously used gates and rewire them
		var nextGate *network.Gate
		nextMod := s.Modules[startIdx+1]
		if isPortPipeline {
			prevPort := s.Modules[startIdx-1]
			prevGate := prevPort.GetEGate(nextMod)
			nextGate = nextMod.GetIGate(prevPort)
			prevPort.Connect(mod, true, prevGate)
			mod.Connect(prevPort, false, nil)
		}
		// Rewire next Module
		mod.Connect(nextMod, true, nil)
		nextMod.Connect(mod, false, nextGate)

		// Request to insert at end of meta or port pipeline
	} else if index >= endIdx || index < 0 {
		// Check for inserting at end
		log.Debug("Inserting Module at end of pipeline")
		prevMod := s.Modules[endIdx]
		if len(s.Modules)-1 != endIdx {
			var prevGate *network.Gate
			// We know this is a port pipeline and there are egress ports
			// Need to wire all egress ports and preserve previous gate usage
			for _, eMod := range s.EgressPorts {
				nextGate := eMod.GetIGate(prevMod)
				prevGate = prevMod.GetEGate(eMod)
				// prevMod will no longer connect to many egress ports, and only connect to the new module
				// reset all their wiring to nil
				prevMod.GetEGateMap()[*prevGate] = nil
				mod.Connect(eMod, true, nil)
				eMod.Connect(mod, false, nextGate)
			}

			// wire previous module
			mod.Connect(prevMod, false, nil)
			// re-use last gate found that was previously wired to egress port
			prevMod.Connect(mod, true, prevGate)

			// Update Modules slice
			s.Modules = append(s.Modules, mod)
			copy(s.Modules[startIdx+1:], s.Modules[startIdx:])
			s.Modules[index] = mod
		} else {
			// meta pipeline, no egress ports. Just append and wire to previous module
			s.Modules = append(s.Modules, mod)
			prevMod.Connect(mod, true, nil)
			mod.Connect(prevMod, false, nil)
		}
		// Must be inserting in the middle
	} else {
		log.Debug("Inserting Module at middle of pipeline")
		// Update Modules slice
		s.Modules = append(s.Modules, mod)
		copy(s.Modules[index+1:], s.Modules[index:])
		s.Modules[index] = mod

		prevMod := s.Modules[index-1]
		nextMod := s.Modules[index+1]
		prevGate := prevMod.GetEGate(nextMod)
		nextGate := nextMod.GetIGate(prevMod)
		prevMod.Connect(mod, true, prevGate)
		mod.Connect(prevMod, false, nil)
		mod.Connect(nextMod, true, nil)
		nextMod.Connect(mod, false, nextGate)
	}

	if isPortPipeline {
		log.Debug("Port Pipeline: Re-rendering pipeline")
		if err := agent.Driver.ReRenderModules(s.Modules); err != nil {
			return err
		}
		log.Debug("Updating FIB for all currently configured egress ports")
		if err := agent.updateFIBFromEgress(s); err != nil {
			return err
		}
		agent.Driver.Commit()
	}

	return nil
}

// RemoveModule removes a module from a pipeline
// agent is a pointer to the agent to use to render the changes when a port pipeline is updated
func (s *NimbessPipeline) RemoveModule(modName string, agent *NimbessAgent) error {
	var mod, prevMod, nextMod network.PipelineModule
	var modIdx int
	for idx, module := range s.Modules {
		if module.GetName() == modName {
			mod = module
			modIdx = idx
			log.Debugf("Module found for Removal: %s", mod.GetName())
			if idx > 0 {
				prevMod = s.Modules[idx-1]
				log.Debugf("Previous Module found: %s", prevMod.GetName())
			}
			if idx < len(s.Modules)-1 {
				nextMod = s.Modules[idx+1]
				log.Debugf("Next Module found: %s", nextMod.GetName())
			}
			break
		}
	}

	if mod == nil {
		return fmt.Errorf("unable to find module to remove: %s", modName)
	}

	if prevMod != nil && nextMod != nil {
		// if both prevMod and nextMod exist, find gates used to connect to mod and rewire them together
		prevGate := prevMod.GetEGate(mod)
		nextGate := nextMod.GetIGate(mod)
		prevMod.Connect(nextMod, true, prevGate)
		nextMod.Connect(prevMod, false, nextGate)
	} else if prevMod != nil {
		// if no next mod, then set the gate to nil
		prevGate := prevMod.GetEGate(mod)
		prevMod.GetEGateMap()[*prevGate] = nil
	} else {
		// if no prev mod, then set the gate to nil
		nextGate := nextMod.GetIGate(mod)
		nextMod.GetIGateMap()[*nextGate] = nil
	}

	// remove module from modules slice
	s.Modules = append(s.Modules[:modIdx], s.Modules[modIdx+1:]...)
	if agent != nil {
		defer agent.Driver.Commit()
		if err := agent.Driver.ReRenderModules(s.Modules); err != nil {
			return err
		}
		log.Debug("Updating FIB for all currently configured egress ports")
		if err := agent.updateFIBFromEgress(s); err != nil {
			return err
		}
		// remove old module
		if err := agent.Driver.DeleteModules([]network.PipelineModule{mod}, false); err != nil {
			return err
		}
	}
	return nil
}

func createModFromMeta(module network.PipelineModule, name string) network.PipelineModule {
	log.Debugf("Instantiating new module from meta module: %+v", module)
	// We can assert Module safely here because every complex module should always inherit Module
	newMod := util.Copy(module).(network.PipelineModule)
	// Unique module names for pipeline use format <Module name>_<port name>
	newName := fmt.Sprintf("%s_%s", newMod.GetName(), name)
	newMod.SetName(newName)
	// Set the meta pipeline name for this module
	newMod.SetMeta(module.GetName())
	// Create new maps for gates, as new mappings will be built recursively
	newMod.SetEGateMap(make(map[network.Gate]network.PipelineModule))
	newMod.SetIGateMap(make(map[network.Gate]network.PipelineModule))
	return newMod
}

func (s *NimbessPipeline) GetEgressPorts() []*network.EgressPort {
	var egressList []*network.EgressPort
	for _, mod := range s.Modules {
		if reflect.TypeOf(mod) == reflect.TypeOf(&network.EgressPort{}) {
			egressList = append(egressList, mod.(*network.EgressPort))
		}
	}
	return egressList
}
