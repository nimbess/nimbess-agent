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

// Main package for the Nimbess agent executable.
package main

import (
	"context"
	"flag"
	"path/filepath"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sync"

	"github.com/nimbess/nimbess-agent/pkg/agent"
	"github.com/nimbess/nimbess-agent/pkg/drivers"
	"github.com/nimbess/nimbess-agent/pkg/drivers/bess"
)

const (
	// LogFile holds the name of Nimbess Agent log file
	LogFile = "nimbess-agent.log"
	// LogDir is the default path for Agent logging
	LogDir = "/var/log/nimbess"
	// ConfigFile holds the default path to the Nimbess Agent configuration file
	ConfigFile = "/etc/nimbess/agent/agent.yaml"
)

// Returns a configured driver object
func selectDriver(config *agent.NimbessConfig) drivers.Driver {
	driverConfig := drivers.DriverConfig{
		NetworkMode: config.Driver,
		MacLearn:    config.MacLearn,
		TunnelMode:  config.TunnelMode,
		FIBSize:     config.FIBSize,
		Port:        config.DataPlanePort,
		PCIDevices:  config.NICs,
		WorkerCores: config.WorkerCores,
	}
	switch config.DataPlane {
	case agent.BESS:
		// TODO (FIXME) Just use background context for now
		driver := &bess.Driver{DriverConfig: driverConfig, Context: context.Background()}
		log.Infof("Driver loaded %+v", driver)
		return driver
	default:
		log.Fatalf("Invalid driver: %s", config.Driver)
	}
	return nil
}

func main() {
	// Determine Agent configuration
	confFile := flag.String("config-file", ConfigFile,
		"Nimbess Agent config file path")
	logDir := flag.String("log-dir", LogDir, "Logging directory path")
	flag.Parse()

	if _, err := os.Stat(*logDir); os.IsNotExist(err) {
		_ = os.Mkdir(*logDir, 0644)
	}
	logFile, e := os.OpenFile(filepath.Join(*logDir, LogFile), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if e != nil {
		panic(e)
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	// TODO make this log level configurable
	log.SetLevel(log.DebugLevel)

	nimbessConfig := agent.InitConfig(*confFile)
	driver := selectDriver(nimbessConfig)
	// Create Pipeline map of per port pipelines
	nimbessPipelineMap := make(map[string]*agent.NimbessPipeline)
	metaPipelineMap := make(map[string]*agent.NimbessPipeline)
	// Generate ID for this Agent
	// TODO(trozet) Make this ID stored in etcd and pull at agent start
	agentID := uuid.New()
	// Start agent
	nimbessAgent := agent.NimbessAgent{Mu: &sync.Mutex{}, Config: nimbessConfig, Driver: driver,
		Pipelines: nimbessPipelineMap, ID: agentID, MetaPipelines: metaPipelineMap}
	if err := nimbessAgent.Run(); err != nil {
		log.Fatalf("Nimbess Agent has died: %v", err)
	}
}