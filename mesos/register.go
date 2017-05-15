package mesos

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/CiscoCloud/mesos-consul/registry"
	"github.com/CiscoCloud/mesos-consul/state"

	log "github.com/sirupsen/logrus"
)

// Query the consul agent on the Mesos Master
// to initialize the cache.
//
// All services created by mesos-consul are prefixed
// with service-id-prefix flag, followed by a colon.
//
func (m *Mesos) LoadCache() error {
	log.Debug("Populating cache from Consul")

	mh := m.getLeader()

	return m.Registry.CacheLoad(mh.Ip, m.ServiceIdPrefix)
}

func (m *Mesos) RegisterHosts(s state.State) {
	log.Debug("Running RegisterHosts")

	m.Agents = make(map[string]string)

	// Register slaves
	for _, f := range s.Slaves {
		agent := toIP(f.PID.Host)
		port := toPort(f.PID.Port)

		m.Agents[f.ID] = agent

		m.registerHost(&registry.Service{
			ID:      fmt.Sprintf("%s:%s:%s:%s", m.ServiceIdPrefix, m.ServiceName, f.ID, f.Hostname),
			Name:    m.ServiceName,
			Port:    port,
			Address: agent,
			Agent:   agent,
			Tags:    m.agentTags("agent", "follower"),
			Check: &registry.Check{
				HTTP:     fmt.Sprintf("http://%s:%d/slave(1)/health", agent, port),
				Interval: "10s",
			},
		})
	}

	// Register masters
	mas := m.getMasters()
	for _, ma := range mas {
		var tags []string

		if ma.IsLeader {
			tags = m.agentTags("leader", "master")
		} else {
			tags = m.agentTags("master")
		}
		s := &registry.Service{
			ID:      fmt.Sprintf("%s:%s:%s:%s", m.ServiceIdPrefix, m.ServiceName, ma.Ip, ma.PortString),
			Name:    m.ServiceName,
			Port:    ma.Port,
			Address: ma.Ip,
			Agent:   ma.Ip,
			Tags:    tags,
			Check: &registry.Check{
				HTTP:     fmt.Sprintf("http://%s:%d/master/health", ma.Ip, ma.Port),
				Interval: "10s",
			},
		}

		m.registerHost(s)
	}
}

func (m *Mesos) registerHost(s *registry.Service) {
	h := m.Registry.CacheLookup(s.ID)
	if h != nil {
		log.Infof("Host found. Comparing tags: (%v, %v)", h.Tags, s.Tags)

		if sliceEq(s.Tags, h.Tags) {
			m.Registry.CacheMark(s.ID)

			// Tags are the same. Return
			return
		}

		log.Info("Tags changed. Re-registering")

		// Delete cache entry. It will be re-created below
		m.Registry.CacheDelete(s.ID)
	}

	m.Registry.Register(s)
}

func (m *Mesos) registerTask(t *state.Task, agent string) error {
	var tags []string

	if _, err := t.Label("consul"); err == nil {
		// For migration purpose
		return errors.New("Application with consul label")
	}

	tname := cleanName(t.ID, m.Separator)
	log.Debugf("original TaskName : (%v)", tname)
	if value, err := t.Label("overrideTaskName"); err == nil {
		tname = cleanName(value, m.Separator)
		log.Debugf("overrideTaskName to : (%v)", tname)
	}
	if !m.TaskPrivilege.Allowed(tname) {
		return errors.New("Task not allowed to be registered")
	}

	address := t.IP(m.IpOrder...)

	if l, err := t.Label("tags"); err == nil {
		tags = strings.Split(l, ",")
	} else {
		tags = []string{}
	}

	tags = buildRegisterTaskTags(tname, tags, m.taskTag)

	if t.DiscoveryInfo.Name != "" {
		// Loop over DiscoveryInfo Ports
		for key, discoveryPort := range t.DiscoveryInfo.Ports.DiscoveryPorts {
			var porttags []string
			serviceName := discoveryPort.Name
			servicePort := strconv.Itoa(discoveryPort.Number)
			pl := discoveryPort.Label("tags")
			if pl != "" {
				porttags = strings.Split(discoveryPort.Label("tags"), ",")
			} else {
				porttags = []string{}
			}
			// Register the first port of the array as the main service
			if key == 0 {
				// TODO: propose an alternative logic, i.e. the register first non-labelled port
				log.Debugf("Will register the first port for Task %+v", t.Name)
				m.Registry.Register(&registry.Service{
					ID:      fmt.Sprintf("mesos-consul:%s:%s:%s", agent, tname, servicePort),
					Name:    tname,
					Port:    toPort(servicePort),
					Address: address,
					Tags:    tags,
					Check: GetCheck(t, &CheckVar{
						Host: toIP(address),
						Port: servicePort,
					}),
					Agent: toIP(agent),
				})
			}
			// Register every named port as a service
			if discoveryPort.Name != "" {
				log.Debugf("%+v framework has %+v as a name for %+v port",
					t.Name,
					discoveryPort.Name,
					discoveryPort.Number)
					named_service := cleanName(tname+"-"+discoveryPort.Name, m.Separator)
				log.Debugf("%+v: changing the service name from %+v to %+v",
					t.Name,
					tname,
					named_service)
				m.Registry.Register(&registry.Service{
					ID:      fmt.Sprintf("%s:%s:%s:%s:%d", m.ServiceIdPrefix, agent, tname, address, discoveryPort.Number),
					Name:    named_service,
					Port:    toPort(servicePort),
					Address: address,
					Tags:    append(append(tags, serviceName), porttags...),
					Check: GetCheck(t, &CheckVar{
						Host: toIP(address),
						Port: servicePort,
					}),
					Agent: toIP(agent),
				})
			}
		}
	} else {
		m.Registry.Register(&registry.Service{
			ID:      fmt.Sprintf("%s:%s-%s:%s", m.ServiceIdPrefix, agent, tname, address),
			Name:    tname,
			Address: address,
			Tags:    tags,
			Check: GetCheck(t, &CheckVar{
				Host: toIP(address),
			}),
			Agent: toIP(agent),
		})
	}
	return nil
}

// buildRegisterTaskTags takes a cleaned task name, a slice of starting tags, and the processed
// taskTag map and returns a slice of tags that should be applied to this task.
func buildRegisterTaskTags(taskName string, startingTags []string, taskTag map[string][]string) []string {
	result := startingTags
	tnameLower := strings.ToLower(taskName)

	for pattern, taskTags := range taskTag {
		for _, tag := range taskTags {
			if strings.Contains(tnameLower, pattern) {
				if !sliceContainsString(result, tag) {
					log.WithField("task-tag", tnameLower).Debug("Task matches pattern for tag")
					result = append(result, tag)
				}
			}
		}
	}

	return result
}

func (m *Mesos) agentTags(ts ...string) []string {
	if len(m.ServiceTags) == 0 {
		return ts
	}

	rval := []string{}

	for _, tag := range m.ServiceTags {
		for _, t := range ts {
			rval = append(rval, fmt.Sprintf("%s.%s", t, tag))
		}
	}

	return rval
}
