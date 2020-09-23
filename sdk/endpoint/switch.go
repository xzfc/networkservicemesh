package endpoint

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"

	"github.com/networkservicemesh/networkservicemesh/controlplane/api/connection"
	"github.com/networkservicemesh/networkservicemesh/controlplane/api/networkservice"
	"github.com/networkservicemesh/networkservicemesh/pkg/tools/spanhelper"
)

// SwitchEndpoint is a NetworkServiceServer composition primitive which passes
// incoming requests into one of its items specified by user-provided rules.
type SwitchEndpoint struct {
	items   []networkservice.NetworkServiceServer
	matcher switchMatcher
}

// A SwitchTest reports whether the connection satisfies an arbitrary condition.
type SwitchTest func(*connection.Connection) bool

// A SwitchArm ties together a condition test and a service which should be used
// when the condition tests returns true.
type SwitchArm struct {
	Test    SwitchTest
	Service networkservice.NetworkServiceServer
}

// A switchMatcher accepts a connection and returns the corresponding network
// service server.
type switchMatcher func(*connection.Connection) (networkservice.NetworkServiceServer, error)

// NewSwitchEndpoint constructs SwitchEndpoint instance using an array of arms.
// Each arm consists of a condition test and a service which should be used when
// the condition test returns true. The first matching arm takes the precedence.
func NewSwitchEndpoint(arms ...SwitchArm) *SwitchEndpoint {
	items := []networkservice.NetworkServiceServer{}
	for _, arm := range arms {
		items = append(items, arm.Service)
	}

	matcher := func(c *connection.Connection) (networkservice.NetworkServiceServer, error) {
		for _, arm := range arms {
			if arm.Test(c) {
				return arm.Service, nil
			}
		}
		return nil, errors.New("NewSwitchEndpoint: no match")
	}

	return &SwitchEndpoint{
		items:   items,
		matcher: matcher,
	}
}

func (s *SwitchEndpoint) Init(context *InitContext) error {
	for _, item := range s.items {
		if err := Init(item, context); err != nil {
			return err
		}
	}
	return nil
}

func (s *SwitchEndpoint) Request(ctx context.Context, request *networkservice.NetworkServiceRequest) (*connection.Connection, error) {
	span := spanhelper.FromContext(ctx, "SwitchEndpoint.Request")
	defer span.Finish()
	e, err := s.matcher(request.Connection)
	if err != nil {
		return nil, err
	}
	return e.Request(ctx, request)
}

func (s *SwitchEndpoint) Close(ctx context.Context, connection *connection.Connection) (*empty.Empty, error) {
	span := spanhelper.FromContext(ctx, "SwitchEndpoint.Close")
	defer span.Finish()
	e, err := s.matcher(connection)
	if err != nil {
		return nil, err
	}
	return e.Close(ctx, connection)
}
