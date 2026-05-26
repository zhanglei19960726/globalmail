package globalmail

import "time"

type ConsistencyMetrics interface {
	IncCounter(name string, labels map[string]string)
	ObserveDuration(name string, duration time.Duration, labels map[string]string)
}

func (c *LocalCache) SetMetrics(metrics ConsistencyMetrics) {
	c.metrics = metrics
}

func (c *LocalCache) incCounter(name string, labels map[string]string) {
	if c.metrics != nil {
		c.metrics.IncCounter(name, labels)
	}
}

func (c *LocalCache) observeDuration(name string, duration time.Duration, labels map[string]string) {
	if c.metrics != nil {
		c.metrics.ObserveDuration(name, duration, labels)
	}
}

func (r *OutboxRelay) incCounter(name string, labels map[string]string) {
	if r.metrics != nil {
		r.metrics.IncCounter(name, labels)
	}
}

func (r *OutboxRelay) observeDuration(name string, duration time.Duration, labels map[string]string) {
	if r.metrics != nil {
		r.metrics.ObserveDuration(name, duration, labels)
	}
}
