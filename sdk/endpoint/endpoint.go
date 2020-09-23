// Copyright 2018, 2019 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package endpoint

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/pkg/errors"

	"github.com/networkservicemesh/networkservicemesh/pkg/tools/jaeger"
	"github.com/networkservicemesh/networkservicemesh/pkg/tools/spanhelper"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/networkservicemesh/networkservicemesh/controlplane/api/connection"
	"github.com/networkservicemesh/networkservicemesh/controlplane/api/networkservice"
	unified "github.com/networkservicemesh/networkservicemesh/controlplane/api/networkservice"
	"github.com/networkservicemesh/networkservicemesh/controlplane/api/registry"
	"github.com/networkservicemesh/networkservicemesh/pkg/tools"
	"github.com/networkservicemesh/networkservicemesh/sdk/common"
)

// NsmEndpoint  provides the grpc mechanics for an NsmEndpoint
type NsmEndpoint interface {
	Start() error
	Delete() error
}

// Registration stores parameters that NsmEndpoint passes to the RegisterNSE
// gRPC call.
type Registration struct {
	Name   string
	Labels map[string]string
}

type nsmEndpoint struct {
	*common.NsmConnection
	service        networkservice.NetworkServiceServer
	grpcServer     *grpc.Server
	registryClient registry.NetworkServiceRegistryClient
	registrations  []endpointRegistration
	tracerCloser   io.Closer
}

type endpointRegistration struct {
	Registration
	registeredName string
}

func (nsme *nsmEndpoint) setupNSEServerConnection() (net.Listener, error) {
	c := nsme.Configuration
	if err := tools.SocketCleanup(c.NsmClientSocket); err != nil {
		logrus.Errorf("nse: failure to cleanup stale socket %s with error: %v", c.NsmClientSocket, err)
		return nil, err
	}

	logrus.Infof("nse: listening socket %s", c.NsmClientSocket)
	connectionServer, err := net.Listen("unix", c.NsmClientSocket)
	if err != nil {
		logrus.Errorf("nse: failure to listen on a socket %s with error: %v", c.NsmClientSocket, err)
		return nil, err
	}
	return connectionServer, nil
}

func (nsme *nsmEndpoint) serve(listener net.Listener) {
	go func() {
		if err := nsme.grpcServer.Serve(listener); err != nil {
			logrus.Fatalf("nse: failed to start grpc server on socket %v with error: %v ", nsme.Configuration.NsmClientSocket, err)
		}
	}()
}

func (nsme *nsmEndpoint) Start() error {
	nsme.tracerCloser = jaeger.InitJaeger(nsme.Configuration.EndpointNetworkService)

	nsme.grpcServer = tools.NewServer(nsme.Context)
	unified.RegisterNetworkServiceServer(nsme.grpcServer, nsme)

	listener, err := nsme.setupNSEServerConnection()
	if err != nil {
		logrus.Errorf("Unable to setup NSE")
		return err
	}

	err = Init(nsme.service, &InitContext{
		GrpcServer: nsme.grpcServer,
	})
	if err != nil {
		return err
	}

	// spawn the listening thread
	nsme.serve(listener)

	nsme.registryClient = registry.NewNetworkServiceRegistryClient(nsme.GrpcClient)
	for i := range nsme.registrations {
		nsme.register(&nsme.registrations[i])
	}

	return nil
}

func (nsme *nsmEndpoint) register(r *endpointRegistration) {
	span := spanhelper.FromContext(nsme.Context, fmt.Sprintf("Endpoint-%v-Start", r.Name))
	span.LogObject("labels", r.Labels)
	defer span.Finish()

	// Registering NSE API, it will listen for Connection requests from NSM and return information
	// needed for NSE's forwarder programming.
	nse := &registry.NetworkServiceEndpoint{
		NetworkServiceName: r.Name,
		Payload:            "IP",
		Labels:             r.Labels,
	}
	registration := &registry.NSERegistration{
		NetworkService: &registry.NetworkService{
			Name:    r.Name,
			Payload: "IP",
		},
		NetworkServiceEndpoint: nse,
	}
	span.LogObject("nse-request", registration)

	registeredNSE, err := nsme.registryClient.RegisterNSE(span.Context(), registration)
	if err != nil {
		span.Logger().Fatalln("unable to register endpoint", err)
	}
	r.registeredName = registeredNSE.GetNetworkServiceEndpoint().GetName()
	span.LogObject("endpoint-name", r.registeredName)
	span.Logger().Infof("NSE registered: %v", registeredNSE)
	span.Logger().Infof("NSE: channel has been successfully advertised, waiting for connection from NSM...")
}

func (nsme *nsmEndpoint) deregister(r *endpointRegistration) error {
	span := spanhelper.FromContext(context.Background(), fmt.Sprintf("Endpoint-%v-Delete", r.Name))
	defer span.Finish()
	// prepare and defer removing of the advertised endpoint
	removeNSE := &registry.RemoveNSERequest{
		NetworkServiceEndpointName: r.registeredName,
	}
	span.LogObject("delete-request", removeNSE)
	_, err := nsme.registryClient.RemoveNSE(span.Context(), removeNSE)
	if err != nil {
		span.Logger().Errorf("Failed removing NSE: %v, with %v", removeNSE, err)
	}
	return err
}

func (nsme *nsmEndpoint) Delete() error {
	var err error
	for i := range nsme.registrations {
		err1 := nsme.deregister(&nsme.registrations[i])
		if err == nil && err1 != nil {
			err = err1
		}
	}
	nsme.grpcServer.Stop()
	_ = nsme.tracerCloser.Close()

	return err
}

func (nsme *nsmEndpoint) Request(ctx context.Context, request *networkservice.NetworkServiceRequest) (*connection.Connection, error) {
	span := spanhelper.FromContext(ctx, "Endpoint.Request")
	defer span.Finish()
	span.LogObject("request", request)

	logger := span.Logger()
	logger.Infof("Request for Network Service received %v", request)

	incomingConnection, err := nsme.service.Request(ctx, request)
	if err != nil {
		logger.Errorf("The composite returned an error: %v", err)
		return nil, err
	}

	logger.Infof("Responding to NetworkService.Request(%v): %v", request, incomingConnection)
	span.LogObject("response", incomingConnection)
	return incomingConnection, nil
}

func (nsme *nsmEndpoint) Close(ctx context.Context, incomingConnection *connection.Connection) (*empty.Empty, error) {
	span := spanhelper.FromContext(ctx, "Endpoint.Close")
	defer span.Finish()
	span.LogObject("connection", incomingConnection)
	_, _ = nsme.service.Close(ctx, incomingConnection)
	_, _ = nsme.NsClient.Close(ctx, incomingConnection)

	return &empty.Empty{}, nil
}

// NewNSMEndpoint creates a new NSM endpoint with a single registration.
func NewNSMEndpoint(ctx context.Context, configuration *common.NSConfiguration, service networkservice.NetworkServiceServer) (NsmEndpoint, error) {
	registration := []Registration{MakeNsmEndpointRegistration(configuration)}
	return NewNSMEndpointWithRegistrations(ctx, configuration, service, registration)
}

// MakeNsmEndpointRegistration extracts Registration from the
// configuration.
func MakeNsmEndpointRegistration(configuration *common.NSConfiguration) Registration {
	return Registration{
		Name:   configuration.EndpointNetworkService,
		Labels: tools.ParseKVStringToMap(configuration.EndpointLabels, ",", "="),
	}
}

// NewNSMEndpointWithRegistrations creates a new NSM endpoint. It's an extended
// version of NewNSMEndpoint that provides a way to specify multiple
// registrations.
func NewNSMEndpointWithRegistrations(ctx context.Context, configuration *common.NSConfiguration,
	service networkservice.NetworkServiceServer, registrations []Registration) (NsmEndpoint, error) {
	if configuration == nil {
		configuration = &common.NSConfiguration{}
	}

	if service == nil {
		return nil, errors.New("NewNSMEndpoint must be provided a non-nil service *networkservice.NewNetworkServiceServer argument")
	}

	nsmConnection, err := common.NewNSMConnection(ctx, configuration)
	if err != nil {
		logrus.Errorf("Error: %v", err)
		return nil, err
	}

	internalRegistrations := make([]endpointRegistration, len(registrations))
	for i := range registrations {
		internalRegistrations[i] = endpointRegistration{
			Registration: registrations[i],
		}
	}

	endpoint := &nsmEndpoint{
		NsmConnection: nsmConnection,
		service:       service,
		registrations: internalRegistrations,
	}

	return endpoint, nil
}
