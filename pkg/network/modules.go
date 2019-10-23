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

// Modules define network function types

// Package network contains all rendered network oriented resources for Nimbess
package network

import (
	"github.com/nimbess/stargazer/pkg/crd/api/unp/v1"
	log "github.com/sirupsen/logrus"
)

// Gate is a virtual port for a module for traffic to ingress or egress
type Gate uint64

type L2FIBCommand struct {
	Command   string
	MAC       string
	Permanent bool
	Setage    int64
	Port      string
}

// PipelineModule type to implement module updates
type PipelineModule interface {
	Connect(module PipelineModule, egress bool, gate *Gate) Gate
	Disconnect(gate Gate, egress bool)
	GetNextIGate() Gate
	GetNextEGate() Gate
	GetName() string
	SetName(s string)
	GetIGateMap() map[Gate]PipelineModule
	SetIGateMap(m map[Gate]PipelineModule)
	GetEGateMap() map[Gate]PipelineModule
	SetEGateMap(m map[Gate]PipelineModule)
	SetMeta(s string)
	GetMeta() string
	GetIGate(module PipelineModule) *Gate
	GetEGate(module PipelineModule) *Gate
}

// Module represents a generic Network Function.
// It contains ingress gates and egress gates for traffic flow, which map to the next connected module.
// Meta field is used to store the name of the Meta Pipeline Module from which this module was created
type Module struct {
	Meta   string
	Name   string
	IGates map[Gate]PipelineModule
	EGates map[Gate]PipelineModule
}

// Switch represents an L2 network switch.
// Extends Module and defines L2FIB table which holds a map of map to output Gates
// EGate 999 is always reserved in Switch for connecting to replicator
type Switch struct {
	Module
	L2FIB map[string]Gate
}

// Port represents a network interface
type Port struct {
	PortName   string
	Virtual    bool
	DPDK       bool
	UnixSocket bool
	IfaceName  string
	Network    string
	NamesSpace string
	IPAddr     string
	MacAddr    string
	Gateway    string
	SocketPath string
}

// IngressPort represents the RX side of a network interface
type IngressPort struct {
	Module
	*Port
}

// EgressPort represents the TX side of a network interface
type EgressPort struct {
	Module
	*Port
}

// URLFilter
type URLFilter struct {
	Module
	v1.URLFilter
}

// GetName returns the Module's name
func (m *Module) GetName() string {
	return m.Name
}

// SetName sets the Module's name
func (m *Module) SetName(s string) {
	m.Name = s
}

// SetMeta sets the name of the Meta Pipeline Module used to create this Module
func (m *Module) SetMeta(s string) {
	m.Meta = s
}

// GetMeta gets the name of the Meta Pipeline Module used to create this Module
func (m *Module) GetMeta() string {
	return m.Meta
}

// Connect creates a connection between modules in either ingress/egress direction.
// module argument should be a pointer to Module type
// egress defines if this connection should apply to egress or ingress gate map
// gate defines a specific gate to use and not automatically get next gate
func (m *Module) Connect(module PipelineModule, egress bool, gate *Gate) Gate {
	var rGate Gate
	if egress {
		if gate != nil {
			rGate = *gate
		} else {
			rGate = m.GetNextEGate()
		}
		m.EGates[rGate] = module
		log.Debugf("Module %s egress gate: %d, wired to module: %s", m.Name, rGate, module.GetName())
	} else {
		if gate != nil {
			rGate = *gate
		} else {
			rGate = m.GetNextIGate()
		}
		m.IGates[rGate] = module
		log.Debugf("Module %s ingress gate: %d, wired to module: %s", m.Name, rGate, module.GetName())
	}
	return rGate
}

// Disconnect removes the connection in a Module's Egress or Ingress Gate Map
func (m *Module) Disconnect(gate Gate, egress bool) {
	if egress {
		delete(m.EGates, gate)
	} else {
		delete(m.IGates, gate)
	}
}

// GetNextIGate returns the next unused Ingress gate
func (m Module) GetNextIGate() Gate {
	return Gate(len(m.IGates))
}

// GetNextEGate returns the next unused Egress gate
func (m Module) GetNextEGate() Gate {
	return Gate(len(m.EGates))
}

// GetIGateMap returns the Ingress Gate Map for Module
func (m Module) GetIGateMap() map[Gate]PipelineModule {
	return m.IGates
}

// GetEGateMap returns the Egress Gate Map for Module
func (m Module) GetEGateMap() map[Gate]PipelineModule {
	return m.EGates
}

// SetIGateMap sets the Ingress Gate Map for a Module
func (m *Module) SetIGateMap(gMap map[Gate]PipelineModule) {
	m.IGates = gMap
}

// SetEGateMap sets the Egress Gate Map for a Module
func (m *Module) SetEGateMap(gMap map[Gate]PipelineModule) {
	m.IGates = gMap
}

// GetIGate gets the corresponding gate
func (m *Module) GetIGate(module PipelineModule) *Gate {
	if module == nil {
		return nil
	}
	return getGate(m.GetIGateMap(), module)

}

// GetEGate gets the corresponding gate
func (m *Module) GetEGate(module PipelineModule) *Gate {
	if module == nil {
		return nil
	}
	return getGate(m.GetEGateMap(), module)
}

func getGate(gateMap map[Gate]PipelineModule, module PipelineModule) *Gate {
	for k, v := range gateMap {
		if v == module {
			return &k
		}
	}

	return nil
}

// Create a Gate Map - will include init for default gates later
func MakeGateMap() map[Gate]PipelineModule {
	rGates := make(map[Gate]PipelineModule)
	return rGates
}
