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
	"github.com/nimbess/nimbess-agent/pkg/network"
)

// L2Forward is a BESS module for MAC based switching
type L2Forward struct {
	network.Switch
}

// Replicate is a BESS module for replicating a single packet to multiple egress gates
type Replicate struct {
	network.Switch
}
