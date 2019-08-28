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

// Package etcdv3 is the etcd wrapper implementation.
package etcdv3

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/nimbess/nimbess-agent/pkg/agent/config"
	"github.com/nimbess/nimbess-agent/pkg/etcdv3/model"
	log "github.com/sirupsen/logrus"
	"strings"
)

type Client interface {
	Create(ctx context.Context, object *model.KVPair) error
	Delete(ctx context.Context, k model.Key) error
	Watch(ctx context.Context, prefix string) clientv3.WatchChan
	Close()
}

type EtcdV3Client struct {
	etcdClient *clientv3.Client
}

func New(config *config.NimbessConfig) (Client, error) {
	log.WithField("endpoints", config.EtcdEndpoints).Info("Connecting to etcd...")
	etcdEndpoints := strings.Split(config.EtcdEndpoints, ",")
	if len(etcdEndpoints) == 0 {
		return nil, errors.New("no etcd endpoints specified")
	}

	etcdConfig := clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: config.EtcdDialTimeout}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %s", err)
	}

	return &EtcdV3Client{etcdClient}, nil
}

func (c *EtcdV3Client) Close() {
	c.etcdClient.Close()
}

func (c *EtcdV3Client) Create(ctx context.Context, d *model.KVPair) error {
	log.WithFields(log.Fields{"key": d.Key.String(), "value": d.Value}).Debug("Create request")

	key, value, err := getKeyValueStrings(d)

	// TODO: add ttl options
	var putOpts []clientv3.OpOption
	txResp, err := c.etcdClient.KV.Txn(ctx).If(
		notFound(key),
	).Then(
		clientv3.OpPut(key, value, putOpts...),
	).Commit()

	if err != nil {
		return err
	}
	if !txResp.Succeeded {
		return NewKeyExistsError(key, 0)
	}
	return nil
}

func (c *EtcdV3Client) Delete(ctx context.Context, k model.Key) error {
	log.WithFields(log.Fields{"key": k.String()}).Debug("Delete request")

	key, err := model.KeyToDefaultDeletePath(k)
	if err != nil {
		return err
	}

	txResp, err := c.etcdClient.KV.Txn(ctx).If(
		found(key),
	).Then(
		clientv3.OpDelete(key),
	).Else(
		clientv3.OpGet(key),
	).Commit()
	if err != nil {
		return err
	}
	if !txResp.Succeeded {
		// TODO return proper error
		return NewKeyExistsError(key, 0)
	}
	return nil
}

// Watch starts a watcher on a prefix and returns the channel
func (c *EtcdV3Client) Watch(ctx context.Context, prefix string) clientv3.WatchChan {
	log.Debugf("Setting up watcher on path prefix: %s", prefix)
	return c.etcdClient.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func found(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "!=", 0)
}

// getKeyValueStrings returns the etcdv3 etcdKey and serialized value calculated from the
// KVPair.
func getKeyValueStrings(d *model.KVPair) (string, string, error) {
	logCxt := log.WithFields(log.Fields{"model-etcdKey": d.Key, "value": d.Value})
	key, err := model.KeyToDefaultPath(d.Key)
	if err != nil {
		logCxt.WithError(err).Error("Failed to convert model-etcdKey to etcdv3 etcdKey")
		return "", "", ErrorDatastoreError{
			Err:        err,
			Identifier: d.Key,
		}
	}
	bytes, err := model.SerializeValue(d)
	if err != nil {
		logCxt.WithError(err).Error("Failed to serialize value")
		return "", "", ErrorDatastoreError{
			Err:        err,
			Identifier: d.Key,
		}
	}

	return key, string(bytes), nil
}
