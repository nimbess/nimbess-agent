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
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	"time"
)

// RawString is used a value type to indicate that the value is a bare non-JSON string
type rawString string
type rawBool bool

var rawStringType = reflect.TypeOf(rawString(""))
var rawBoolType = reflect.TypeOf(rawBool(true))

type Key interface {
	defaultPath() (string, error)

	// defaultDeletePath() returns a common path representation used by etcdv3
	// and other datastores to delete the object.
	defaultDeletePath() (string, error)

	// valueType returns the object type associated with this key.
	valueType() (reflect.Type, error)

	// String returns a unique string representation of this key.  The string
	// returned by this method must uniquely identify this Key.
	String() string
}

// KeyToDefaultPath converts one of the Keys from this package into a unique
// '/'-delimited path, which is suitable for use as the key when storing the
// value in a hierarchical (i.e. one with directories and leaves) key/value
// datastore such as etcd v3.
//
// Each unique key returns a unique path.
//
// Keys with a hierarchical relationship share a common prefix.  However, in
// order to support datastores that do not support storing data at non-leaf
// nodes in the hierarchy (such as etcd v3), the path returned for a "parent"
// key, is not a direct ancestor of its children.
func KeyToDefaultPath(key Key) (string, error) {
	return key.defaultPath()
}

// KeyToDefaultDeletePath converts one of the Keys from this package into a
// unique '/'-delimited path, which is suitable for use as the key when
// (recursively) deleting the value from a hierarchical (i.e. one with
// directories and leaves) key/value datastore such as etcd v3.
//
// KeyToDefaultDeletePath returns a different path to KeyToDefaultPath when
// it is a passed a Key that represents a non-leaf which, for example, has its
// own metadata but also contains other resource types as children.
//
// KeyToDefaultDeletePath returns the common prefix of the non-leaf key and
// its children so that a recursive delete of that key would delete the
// object itself and any children it has.
func KeyToDefaultDeletePath(key Key) (string, error) {
	return key.defaultDeletePath()
}

// KVPair holds a typed key and value object as well as datastore specific
// revision information.
//
// The Value is dependent on the Key, but in general will be on of the following
// types:
// -  A pointer to a struct
// -  A slice or map
// -  A bare string, boolean value or IP address (i.e. without quotes, so not
//    JSON format).
type KVPair struct {
	Key      Key
	Value    interface{}
	Revision string
	UID      *types.UID
	TTL      time.Duration // For writes, if non-zero, key has a TTL.
}

// ParseValue parses the default JSON representation of our data into one of
// our value structs, according to the type of key.  I.e. if passed a
// PolicyKey as the first parameter, it will try to parse rawData into a
// Policy struct.
func ParseValue(key Key, rawData []byte) (interface{}, error) {
	valueType, err := key.valueType()
	if err != nil {
		return nil, err
	}
	if valueType == rawStringType {
		return string(rawData), nil
	}
	if valueType == rawBoolType {
		return string(rawData) == "true", nil
	}
	value := reflect.New(valueType)
	elem := value.Elem()
	if elem.Kind() == reflect.Struct && elem.NumField() > 0 {
		if elem.Field(0).Type() == reflect.ValueOf(key).Type() {
			elem.Field(0).Set(reflect.ValueOf(key))
		}
	}
	iface := value.Interface()
	err = json.Unmarshal(rawData, iface)
	if err != nil {
		//log.Warningf("Failed to unmarshal %#v into value %#v",
		//	string(rawData), value)
		return nil, err
	}

	if elem.Kind() != reflect.Struct {
		// Pointer to a map or slice, unwrap.
		iface = elem.Interface()
	}
	return iface, nil
}

// Serialize a value in the model to a []byte to stored in the datastore.  This
// performs the opposite processing to ParseValue()
func SerializeValue(d *KVPair) ([]byte, error) {
	valueType, err := d.Key.valueType()
	if err != nil {
		return nil, err
	}
	if d.Value == nil {
		return json.Marshal(nil)
	}
	if valueType == rawStringType {
		return []byte(d.Value.(string)), nil
	}
	if valueType == rawBoolType {
		return []byte(fmt.Sprint(d.Value)), nil
	}
	return json.Marshal(d.Value)
}
