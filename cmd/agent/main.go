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
	"fmt"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sync"

	"github.com/nimbess/nimbess-agent/pkg/agent"
	"github.com/nimbess/nimbess-agent/pkg/agent/config"
	"github.com/nimbess/nimbess-agent/pkg/drivers"
	"github.com/nimbess/nimbess-agent/pkg/drivers/bess"
	"github.com/nimbess/nimbess-agent/pkg/etcdv3"
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
func selectDriver(conf *config.NimbessConfig) drivers.Driver {
	driverConfig := drivers.DriverConfig{
		NetworkMode: conf.Driver,
		MacLearn:    conf.MacLearn,
		TunnelMode:  conf.TunnelMode,
		FIBSize:     conf.FIBSize,
		Port:        conf.DataPlanePort,
		WorkerCores: conf.WorkerCores,
	}
	switch conf.DataPlane {
	case config.BESS:
		// TODO (FIXME) Just use background context for now
		driver := bess.NewDriver(driverConfig, context.Background())
		//driver := &bess.Driver{DriverConfig: driverConfig, Context: context.Background()}
		log.Infof("Driver loaded %+v", driver)
		return driver
	default:
		log.Fatalf("Invalid driver: %s", conf.Driver)
	}
	return nil
}

// getK8SClient builds and returns a Kubernetes client.
func getK8SClient(kubeconfig string) (kubernetes.Interface, error) {
	// Build the kubeconfig.
	if kubeconfig == "" {
		log.Info("Using inClusterConfig")
	}
	k8sConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %s", err)
	}

	// Get Kubernetes clientset.
	k8sClientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubernetes clientset: %s", err)
	}

	return k8sClientset, nil
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
	log.SetReportCaller(true)
	nimbessConfig := config.InitConfig(*confFile)
	driver := selectDriver(nimbessConfig)
	// Create Pipeline map of per port pipelines
	nimbessPipelineMap := make(map[string]*agent.NimbessPipeline)
	metaPipelineMap := make(map[string]*agent.NimbessPipeline)

	// get k8s client for annotating pods
	k8sClient, err := getK8SClient(nimbessConfig.Kubeconfig)
	if err != nil {
		log.WithError(err).Fatal("Failed to get k8s client api")
	}

	// get etcd client
	etcdClient, err := etcdv3.New(nimbessConfig)
	if err != nil {
		log.Fatalf("failed to get etcd client: %v", err)
	}

	// Create and Start agent
	nimbessAgent := agent.NimbessAgent{Mu: &sync.Mutex{}, Config: nimbessConfig, Driver: driver,
		KubeClient: k8sClient, Pipelines: nimbessPipelineMap, MetaPipelines: metaPipelineMap,
		EtcdClient: etcdClient, EtcdContext: context.Background(),
	}

	if err := nimbessAgent.Init(); err != nil {
		log.Fatalf("Failed to initialize Nimbess Agent: %v", err)
	}

	if err := nimbessAgent.Run(); err != nil {
		log.Fatalf("Nimbess Agent has died: %v", err)
	}
}
