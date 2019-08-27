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

package model

import (
	"fmt"
	"github.com/nimbess/stargazer/pkg/errors"
	"reflect"
)

var (
	typeNode = reflect.TypeOf(Agent{})
)

type Agent struct {
	LastStart string `json:"last_start,omitempty"`
}

type AgentKey struct {
	MachineID string
}

func (key AgentKey) defaultDeletePath() (string, error) {
	return key.defaultPath()
}

func (key AgentKey) defaultPath() (string, error) {
	if key.MachineID == "" {
		return "", errors.ErrorInsufficientIdentifiers{Name: "MachineID"}
	}
	return fmt.Sprintf("/nimbess/agents/%s", key.MachineID), nil
}

func (key AgentKey) valueType() (reflect.Type, error) {
	return typeNode, nil
}

func (key AgentKey) String() string {
	return fmt.Sprintf("MachineID(name=%s)", key.MachineID)
}

func (key AgentKey) KeyToDefaultDeletePath() (string, error) {
	return key.defaultPath()
}
