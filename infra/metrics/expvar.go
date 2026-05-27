package metrics

import (
	"expvar"
	"sort"
	"strings"
	"time"
)

type ExpvarMetrics struct {
	counters  *expvar.Map
	durations *expvar.Map
}

func NewExpvarMetrics(namespace string) *ExpvarMetrics {
	if namespace == "" {
		namespace = "globalmail"
	}
	return &ExpvarMetrics{
		counters:  expvar.NewMap(namespace + "_counters"),
		durations: expvar.NewMap(namespace + "_durations"),
	}
}

func (m *ExpvarMetrics) IncCounter(name string, labels map[string]string) {
	m.counters.Add(metricKey(name, labels), 1)
}

func (m *ExpvarMetrics) ObserveDuration(name string, duration time.Duration, labels map[string]string) {
	key := metricKey(name, labels)
	m.durations.Add(key+"_count", 1)
	m.durations.Add(key+"_sum_milliseconds", duration.Milliseconds())
}

func metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, name)
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, "|")
}
