package mesos

import (
	"testing"

	"github.com/CiscoCloud/mesos-consul/state"
)

func TestRegisterTask(t *testing.T) {
	var registerTests = []struct {
		*state.Task
		expected string
	}{
		{task(labels("consul", "")), "Application with consul label"},
	}
	m := new(Mesos)
	for _, rt := range registerTests {
		actual := m.registerTask(rt.Task, "")
		if actual.Error() != rt.expected {
			t.Errorf("expected %v, actual %v", rt.expected, actual)
		}
	}
}

type (
	taskOpt func(*state.Task)
)

func task(opts ...taskOpt) *state.Task {
	var t state.Task
	for _, opt := range opts {
		opt(&t)
	}
	return &t
}

func labels(kvs ...string) taskOpt {
	if len(kvs)%2 != 0 {
		panic("odd number")
	}
	return func(t *state.Task) {
		for i := 0; i < len(kvs); i += 2 {
			t.Labels = append(t.Labels, state.Label{Key: kvs[i], Value: kvs[i+1]})
		}
	}
}
