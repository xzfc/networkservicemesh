package endpoint

import (
	"github.com/networkservicemesh/networkservicemesh/controlplane/api/connection"
)

// SwitchMatchByName returns SwitchTest that reports whether the connection has
// the specified network service name.
func SwitchMatchByName(name string) SwitchTest {
	return func(c *connection.Connection) bool {
		return c.NetworkService == name
	}
}

// SwitchMatchByLabel returns SwitchTest that reports whether the connection has
// the specified label with the specified value.
func SwitchMatchByLabel(name, value string) SwitchTest {
	return func(c *connection.Connection) bool {
		v, ok := c.Labels[name]
		return ok && v == value
	}
}

// SwitchMatchAll returns SwitchTest that matches all connections
// unconditionally.
func SwitchMatchAll() SwitchTest {
	return func(*connection.Connection) bool {
		return true
	}
}

type SwitchMatchConfig struct {
	// Name should match the endpoint name. Ignored if an empty string.
	Name string
	// Labels is an selector for endpoint labels. Ignored if nil.
	Labels map[string]string
}

func SwitchMatchByConfig(config SwitchMatchConfig) SwitchTest {
	return func(connection *connection.Connection) bool {
		if config.Name != "" && connection.NetworkService != config.Name {
			return false
		}
		if config.Labels != nil && !labelsMatch(config.Labels, connection.Labels) {
			return false
		}
		return true
	}
}

// labelsMatch reports whether the selector matches the list. That is, the
// selector is a subset of the list.
func labelsMatch(selector, list map[string]string) bool {
	for key, selectorValue := range selector {
		listValue, ok := list[key]
		if !ok || listValue != selectorValue {
			return false
		}
	}
	return true
}
