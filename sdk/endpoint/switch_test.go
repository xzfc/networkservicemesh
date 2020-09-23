package endpoint

import (
	"context"
	"github.com/onsi/gomega"
	"github.com/pkg/errors"
	"testing"

	"github.com/networkservicemesh/networkservicemesh/controlplane/api/connection"
	"github.com/networkservicemesh/networkservicemesh/controlplane/api/networkservice"
	"github.com/networkservicemesh/networkservicemesh/pkg/tools"
)

func TestSwitchEndpoint(t *testing.T) {
	g := gomega.NewWithT(t)

	serviceA := makeMockEndpoint("a")
	serviceB := makeMockEndpoint("b")
	serviceC := makeMockEndpoint("c")

	switchEndpoint := NewSwitchEndpoint([]SwitchArm{
		{SwitchMatchByName("name"), serviceA},
		{SwitchMatchByLabel("label1", "value1"), serviceB},
		{SwitchMatchByLabel("label2", "value2"), serviceC},
	}...)

	testSwitchEndpoint(g, switchEndpoint)
}

func TestSwitchEndpointMatcher(t *testing.T) {
	g := gomega.NewWithT(t)

	serviceA := makeMockEndpoint("a")
	serviceB := makeMockEndpoint("b")
	serviceC := makeMockEndpoint("c")

	switchEndpoint := &SwitchEndpoint{
		items: []networkservice.NetworkServiceServer{serviceA, serviceB, serviceC},
		matcher: func(connection *connection.Connection) (networkservice.NetworkServiceServer, error) {
			if connection.NetworkService == "name" {
				return serviceA, nil
			}
			if connection.Labels["label1"] == "value1" {
				return serviceB, nil
			}
			if connection.Labels["label2"] == "value2" {
				return serviceC, nil
			}
			return nil, errors.New("no match")
		}}

	testSwitchEndpoint(g, switchEndpoint)
}

func testSwitchEndpoint(g *gomega.WithT, switchEndpoint *SwitchEndpoint) {
	var testItems = []struct {
		connName, connLabels, expectedRes string
		expectedErr                       bool
	}{
		{"name", "", "(a)", false},
		{"name", "label1=value1", "(a)", false},
		{"smth", "label1=value1", "(b)", false},
		{"smth", "label2=value2", "(c)", false},
		{"smth", "label1=x,label2=value2", "(c)", false},
		{"smth", "label3=value3", "", true},
		{"smth", "label1=value1,label2=value2", "(b)", false},
		{"smth", "", "", true},
	}

	for _, it := range testItems {
		request := &networkservice.NetworkServiceRequest{
			Connection: &connection.Connection{
				NetworkService: it.connName,
				Labels:         tools.ParseKVStringToMap(it.connLabels, ",", "="),
			},
		}
		result, err := switchEndpoint.Request(context.Background(), request)
		if it.expectedErr {
			g.Expect(result).To(gomega.BeNil())
			g.Expect(err).NotTo(gomega.BeNil())
		} else {
			g.Expect(result).NotTo(gomega.BeNil())
			g.Expect(result).To(gomega.BeIdenticalTo(request.Connection))
			g.Expect(result.Labels["res"]).To(gomega.Equal(it.expectedRes))
			g.Expect(err).To(gomega.BeNil())
		}
	}
}

func makeMockEndpoint(name string) networkservice.NetworkServiceServer {
	return NewCustomFuncEndpoint(name, func(_ context.Context, c *connection.Connection) error {
		c.Labels["res"] += "(" + name + ")"
		return nil
	})
}
