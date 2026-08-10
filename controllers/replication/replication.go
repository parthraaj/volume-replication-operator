/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replication

import (
	replicationlib "github.com/csi-addons/spec/lib/go/replication"
	"github.com/csi-addons/volume-replication-operator/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Replication represents the instance of a single replication operation.
type Replication struct {
	Params CommonRequestParameters
	Force  bool
}

// Response is the response of a replication operation.
type Response struct {
	Response interface{}
	Error    error
}

// CommonRequestParameters holds the common parameters across replication operations.
type CommonRequestParameters struct {
	ReplicationSource *replicationlib.ReplicationSource
	ReplicationID     string
	Parameters        map[string]string
	Secrets           map[string]string
	Replication       client.VolumeReplication
}

func (r *Replication) Enable() *Response {
	resp, err := r.Params.Replication.EnableVolumeReplication(
		r.Params.ReplicationSource,
		r.Params.ReplicationID,
		r.Params.Secrets,
		r.Params.Parameters,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Replication) Disable() *Response {
	resp, err := r.Params.Replication.DisableVolumeReplication(
		r.Params.ReplicationSource,
		r.Params.ReplicationID,
		r.Params.Secrets,
		r.Params.Parameters,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Replication) Promote() *Response {
	resp, err := r.Params.Replication.PromoteVolume(
		r.Params.ReplicationSource,
		r.Params.ReplicationID,
		r.Force,
		r.Params.Secrets,
		r.Params.Parameters,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Replication) Demote() *Response {
	resp, err := r.Params.Replication.DemoteVolume(
		r.Params.ReplicationSource,
		r.Params.ReplicationID,
		r.Params.Secrets,
		r.Params.Parameters,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Replication) Resync() *Response {
	resp, err := r.Params.Replication.ResyncVolume(
		r.Params.ReplicationSource,
		r.Params.ReplicationID,
		r.Force,
		r.Params.Secrets,
		r.Params.Parameters,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Replication) GetInfo() *Response {
	resp, err := r.Params.Replication.GetVolumeReplicationInfo(
		r.Params.ReplicationSource,
		r.Params.ReplicationID,
		r.Params.Secrets,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Replication) GetDestinationInfo() *Response {
	resp, err := r.Params.Replication.GetReplicationDestinationInfo(
		r.Params.ReplicationSource,
		r.Params.Secrets,
	)

	return &Response{Response: resp, Error: err}
}

func (r *Response) HasKnownGRPCError(knownErrors []codes.Code) bool {
	if r.Error == nil {
		return false
	}

	grpcStatus, ok := status.FromError(r.Error)
	if !ok {
		// This is not gRPC error. The operation must have failed before gRPC
		// method was called, otherwise we would get gRPC error.
		return false
	}

	for _, e := range knownErrors {
		if grpcStatus.Code() == e {
			return true
		}
	}

	return false
}

// GetMessageFromError returns the message from the error.
func GetMessageFromError(err error) string {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		// This is not gRPC error. The operation must have failed before gRPC
		// method was called, otherwise we would get gRPC error.
		return err.Error()
	}

	return grpcStatus.Message()
}
