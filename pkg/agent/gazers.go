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

// Watchingers and Handlers for stargazer

package agent

import (
	"context"
	"fmt"
	v1 "github.com/nimbess/stargazer/pkg/crd/api/unp/v1"

	"github.com/coreos/etcd/clientv3"
	"github.com/nimbess/stargazer/pkg/model"
	log "github.com/sirupsen/logrus"
	"path"

	"github.com/nimbess/nimbess-agent/pkg/etcdv3"
	"github.com/nimbess/nimbess-agent/pkg/network"
)

const (
	ALL_NETWORKS = "ALL_NETWORKS"
)

type Handler interface {
	ProcessEvent(event *clientv3.Event, s *NimbessAgent)
	Init(context.Context, etcdv3.Client) error
	GetChannel() clientv3.WatchChan
}

type UNPGazer struct {
	Path      string
	WatchChan clientv3.WatchChan
}

func (u *UNPGazer) ProcessEvent(event *clientv3.Event, s *NimbessAgent) {
	// Since processing is async of events, if event failure occurs we need to handle it gracefully
	// TODO(trozet): implement handling event failures/feedback mechanism

	k := model.UNPKey{Name: string(event.Kv.Key)}
	log.Infof("key is %s", k.Name)
	if event.IsCreate() {
		unp, err := model.ParseValue(k, event.Kv.Value)
		if err != nil {
			log.Errorf("Failed to unpack UNP: %v, error: %v", event.Kv.Value, err)
		}
		log.Debugf("Successfully unpacked: %+v", unp)
		if err := u.ProcessUNP(unp.(*v1.UnifiedNetworkPolicy), s, true); err != nil {
			log.Errorf("Error during handling Add: %v", err)
		}

	} else if event.IsModify() {
		// TODO(trozet): We don't support updating the policy yet
		log.Debugf("Skipping modify event")
	} else if event.Type.String() == "DELETE" {
		unp, err := model.ParseValue(k, event.PrevKv.Value)
		if err != nil {
			log.Errorf("Failed to unpack UNP: %v, error: %v", event.Kv.Value, err)
		}
		log.Debugf("Successfully unpacked: %+v", unp)
		if err := u.ProcessUNP(unp.(*v1.UnifiedNetworkPolicy), s, false); err != nil {
			log.Errorf("Error during handling Delete: %v", err)
		}
	} else {
		log.Errorf("Unknown event type: %s", event.Type.String())
	}
}

func (u *UNPGazer) ProcessUNP(unp *v1.UnifiedNetworkPolicy, s *NimbessAgent, add bool) error {
	if add {
		log.Debugf("Processing ADD for UNP: %+v", unp)
	} else {
		log.Debugf("Processing DEL for UNP: %+v", unp)
	}
	policyNetwork := ALL_NETWORKS
	if unp.Spec.Network != "" {
		policyNetwork = unp.Spec.Network
	}

	// TODO(trozet): handle pod selection, need to do lookup in stargazer and add pods to model

	// Protect pipelines
	s.Mu.Lock()
	defer s.Mu.Unlock()

	for _, l7Policy := range unp.Spec.L7Policies {
		// Default action should be a constant deny/allow
		// TODO(trozet) fix default action to constant in stargazer
		if l7Policy.Default.Action != "allow" {
			log.Warn("Default policy other than allow is not currently supported, will use allow")

		}

		// Process other attributes of the policy (only URL Filter is supported now)
		subPolicyNetwork := policyNetwork
		if l7Policy.UrlFilter.Network != "" {
			subPolicyNetwork = l7Policy.UrlFilter.Network
		}

		// Check if URL Filter is defined
		if len(l7Policy.UrlFilter.Urls) == 0 {
			log.Debugf("Ignoring URL Filter Policy for UNP :%+v", l7Policy)
			continue
		}

		// Check if specific network was determined and ensure it exists in meta pipeline
		// If it doesn't, then check network-attachment CRDs and create it
		var affectedMetaPipelines []*NimbessPipeline
		if subPolicyNetwork == ALL_NETWORKS {
			for k, v := range s.MetaPipelines {
				log.Debugf("UNP policy %s applies to network: %s", unp.Name, k)
				affectedMetaPipelines = append(affectedMetaPipelines, v)
			}
		} else {
			if _, err := s.EnsureNetwork(subPolicyNetwork); err != nil {
				return err
			}
			affectedMetaPipelines = append(affectedMetaPipelines, s.MetaPipelines[subPolicyNetwork])
		}

		for _, metaPipeline := range affectedMetaPipelines {
			if add {
				// insert module into all meta pipelines
				// create meta pipeline module
				urlMod := &network.URLFilter{
					Module: network.Module{
						// Naming schema for URLFilter modules
						Name:   fmt.Sprintf("%s-%s-%s", unp.Name, "URLFilter", metaPipeline.MetaKey),
						Meta:   metaPipeline.MetaKey,
						IGates: make(map[network.Gate]network.PipelineModule),
						EGates: make(map[network.Gate]network.PipelineModule),
					},
					URLFilter: l7Policy.UrlFilter,
				}
				// URL Filter should be inserted at beginning of pipeline
				if err := metaPipeline.InsertModule(urlMod, 0, s); err != nil {
					log.Errorf("Error while inserting meta module into pipeline %s", metaPipeline.Name)
					return err
				}
			} else {
				if err := metaPipeline.RemoveModule(
					fmt.Sprintf("%s-%s-%s", unp.Name, "URLFilter", metaPipeline.MetaKey),
					nil); err != nil {
					return err
				}
			}
		}
		// find all affected Nimbess pipelines
		// insert module into all existing nimbess pipelines
		for k, v := range s.Pipelines {
			modifyPipe := false
			if subPolicyNetwork == ALL_NETWORKS {
				modifyPipe = true
			} else if v.MetaKey == subPolicyNetwork {
				modifyPipe = true
			}

			if modifyPipe {
				log.Debugf("UNP policy %s applies to port pipeline: %s", unp.Name, k)
				if add {
					mod := createModFromMeta(s.MetaPipelines[v.MetaKey].Modules[0], k)
					if err := s.Pipelines[k].InsertModule(mod, 0, s); err != nil {
						log.Error("Error while inserting module into port pipeline")
						return err
					}
				} else {
					if err := s.Pipelines[k].RemoveModule(
						fmt.Sprintf("%s-%s-%s_%s", unp.Name, "URLFilter", v.MetaKey, k),
						s); err != nil {
						log.Error("Error while removing module from port pipeline")
						return err
					}
				}
			}
		}
	}

	log.Debug("Process UNP complete")
	return nil
}

func (u *UNPGazer) GetChannel() clientv3.WatchChan {
	return u.WatchChan
}

func (u *UNPGazer) Init(ctx context.Context, client etcdv3.Client) error {
	// TODO(trozet) fix stargazer to make the default path a constant
	k := model.UNPKey{Name: "dummy"}
	p, err := k.KeyToDefaultDeletePath()
	if err != nil {
		return err
	}
	u.Path, _ = path.Split(p)
	u.WatchChan = client.Watch(ctx, u.Path)
	return nil
}

func (s *NimbessAgent) runGazers() error {
	gazers := []Handler{&UNPGazer{}}

	log.Debug("Initializing gazers")
	for _, gazer := range gazers {
		if err := gazer.Init(s.EtcdContext, s.EtcdClient); err != nil {
			log.Errorf("Failed to initialize gazer: %+v", gazer)
			return err
		}
	}

	// Run gazers
	// TODO(trozet) add handling for if watcher is closed unexpectedly
	for _, gazer := range gazers {
		go func(g Handler) {
			log.Debug("Gazer started: %+v", g)
			for watchResp := range g.GetChannel() {
				for _, event := range watchResp.Events {
					log.Debugf("Event received! %s executed on %q with value %q\n", event.Type, event.Kv.Key,
						event.Kv.Value)
					g.ProcessEvent(event, s)
				}
			}
		}(gazer)
	}

	return nil
}
