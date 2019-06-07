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

// Defines Nimbess agent configuration.
package agent

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	L2DriverMode = "layer2"
	BESS = "BESS"
)

// Nimbess Agent Network Configuration
type Network struct {
	Driver		string	`mapstructure:"driver"`
	MacLearn	bool	`mapstructure:"mac_learning"`
	TunnelMode	bool	`mapstructure:"tunnel_mode"`
	FIBSize		int64	`mapstructure:"fib_size"`
}

// Generic Nimbess Agent Configuration
type NimbessConfig struct {
	Port			int			`mapstructure:"agent_port"`
	DataPlane		string		`mapstructure:"data_plane"`
	DataPlanePort	int			`mapstructure:"data_plane_port"`
	WorkerCores		[]int64		`mapstructure:"worker_cores"`
	NICs			[]string	`mapstructure:"pci_devices"`
	Network
}

// Load Nimbess Agent config file into a configuration struct
func InitConfig(cfgPath string) *NimbessConfig {
	cfg := &NimbessConfig {
		Port: 9111,
		DataPlane: BESS,
		DataPlanePort: 10514,
		WorkerCores: []int64{0},
		Network: Network{
			Driver: L2DriverMode,
			MacLearn: false,
			TunnelMode: false,
			FIBSize: 1024,
		},
	}
	viper.SetConfigFile(cfgPath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		log.Warningf("Unable to read Nimbess config file: %v, will use defaults", err)
		return cfg
	} else {
		log.Infof("Configuration file found: %s", cfgPath)
	}
	err = viper.Unmarshal(cfg)
	if err != nil {
		log.Fatalf("Unable to parse Nimbess config: %v", err)
	} else {
		log.Infof("Configuration parsed as: +%v", cfg)
	}
	return cfg
}
