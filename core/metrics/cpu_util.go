// Copyright 2025, 2026 The HuaTuo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ccfos/huatuo/internal/cgroups"
	"github.com/ccfos/huatuo/internal/cgroups/stats"
	"github.com/ccfos/huatuo/internal/log"
	"github.com/ccfos/huatuo/internal/pod"
	"github.com/ccfos/huatuo/internal/tracing"
	"github.com/ccfos/huatuo/internal/utils/cpuutil"
	"github.com/ccfos/huatuo/pkg/metric"
)

type cpuUtilStat struct {
	lastUsage     stats.CpuUsage
	lastTimestamp time.Time
	totalUtil     float64
	sysUtil       float64
	usrUtil       float64
	// ready is false until a second sample establishes a real rate
	// window. The first sample only records the baseline.
	ready bool
}

type cpuUtilCollector struct {
	cgroup       cgroups.Cgroup
	numCores     float64
	cpuDataCache cpuUtilStat
	mutex        sync.Mutex
}

func init() {
	tracing.RegisterEventTracing("cpu_util", newCpuCollector)
	_ = pod.RegisterContainerLifeResources("collector_cpu_util", reflect.TypeOf(&cpuUtilStat{}))
}

func newCpuCollector() (*tracing.EventTracingAttr, error) {
	cgroup, err := cgroups.NewManager()
	if err != nil {
		return nil, err
	}

	numCores, err := cpuutil.ParseOnlineCores(cpuutil.SystemCPUOnlinePath)
	if err != nil {
		return nil, fmt.Errorf("read online cpu: %w", err)
	}
	if numCores == 0 {
		return nil, errors.New("no online cpu")
	}

	return &tracing.EventTracingAttr{
		TracingData: &cpuUtilCollector{
			numCores: float64(numCores),
			cgroup:   cgroup,
		},
		Flag: tracing.FlagMetric,
	}, nil
}

// computeCPUUtil converts two cumulative CPU samples into utilization
// percentages. ok is false when the interval cannot produce a meaningful
// rate: counter regression, a non-positive elapsed window, or deltas that
// exceed the wall-clock capacity of the sample.
func computeCPUUtil(prev, curr stats.CpuUsage, elapsed time.Duration, numCores float64) (total, usr, sys float64, ok bool) {
	if elapsed <= 0 || numCores <= 0 {
		return 0, 0, 0, false
	}

	// Usage, User, and System should increase monotonically. This defensive
	// check prevents an unexpected reset from causing uint64 underflow.
	if curr.Usage < prev.Usage ||
		curr.User < prev.User ||
		curr.System < prev.System {
		return 0, 0, 0, false
	}

	deltaTotalTime := curr.Usage - prev.Usage
	deltaUsrTime := curr.User - prev.User
	deltaSysTime := curr.System - prev.System
	// CpuUsage fields are microseconds; keep the denominator in the same unit.
	deltaRealWorldTime := numCores * float64(elapsed.Microseconds())

	if (float64(deltaTotalTime) > deltaRealWorldTime) || (float64(deltaUsrTime+deltaSysTime) > deltaRealWorldTime) {
		return 0, 0, 0, false
	}

	total = float64(deltaTotalTime) * 100 / deltaRealWorldTime
	usr = float64(deltaUsrTime) * 100 / deltaRealWorldTime
	sys = float64(deltaSysTime) * 100 / deltaRealWorldTime
	return total, usr, sys, true
}

func (c *cpuUtilCollector) updateDataCache(cache *cpuUtilStat, container *pod.Container, numCores float64) error {
	var cgroupPath string

	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	if now.Sub(cache.lastTimestamp).Nanoseconds() < 1000000000 {
		return nil
	}

	if container != nil {
		cgroupPath = container.CgroupPath
	}

	stat, err := c.cgroup.CpuUsage(cgroupPath)
	if err != nil {
		return err
	}

	// The first sample only records a baseline. Lifetime counters divided by
	// wall-clock-since-zero report ~0% regardless of actual load, so skip
	// publishing utilization until a real interval exists.
	if cache.lastTimestamp.IsZero() {
		cache.lastUsage = *stat
		cache.lastTimestamp = now
		return nil
	}

	totalUtil, usrUtil, sysUtil, ok := computeCPUUtil(
		cache.lastUsage, *stat, now.Sub(cache.lastTimestamp), numCores,
	)
	cache.lastUsage = *stat
	cache.lastTimestamp = now
	if !ok {
		return nil
	}

	cache.totalUtil = totalUtil
	cache.usrUtil = usrUtil
	cache.sysUtil = sysUtil
	cache.ready = true
	return nil
}

func (c *cpuUtilCollector) updateHostDataCache() ([]*metric.Data, error) {
	if err := c.updateDataCache(&c.cpuDataCache, nil, c.numCores); err != nil {
		return nil, err
	}
	if !c.cpuDataCache.ready {
		return nil, nil
	}

	return []*metric.Data{
		metric.NewGaugeData("usr", c.cpuDataCache.usrUtil, "cpu usr for the host", nil),
		metric.NewGaugeData("sys", c.cpuDataCache.sysUtil, "cpu sys for the host", nil),
		metric.NewGaugeData("total", c.cpuDataCache.totalUtil, "cpu total for the host", nil),
	}, nil
}

func (c *cpuUtilCollector) Update() ([]*metric.Data, error) {
	metrics := []*metric.Data{}

	containers, err := pod.ContainersByType(pod.ContainerTypeNormal | pod.ContainerTypeSidecar)
	if err != nil {
		return nil, err
	}

	for _, container := range containers {
		cpuQuota, err := c.cgroup.CpuQuotaAndPeriod(container.CgroupPath)
		if err != nil {
			log.Infof("cpu quota: container=%s err=%v", container, err)
			continue
		}

		numCores, err := cpuutil.BoundCores(
			cpuQuota.Quota, cpuQuota.Period,
			cpuQuota.EffectiveCPUCount, uint64(c.numCores),
		)
		if err != nil {
			log.Infof("cpu capacity: container=%s err=%v", container, err)
			continue
		}

		dataCache, ok := container.LifeResources("collector_cpu_util").(*cpuUtilStat)
		if !ok || dataCache == nil {
			log.Warnf("cpu cache: container=%s unavailable", container)
			continue
		}
		if err := c.updateDataCache(dataCache, container, numCores); err != nil {
			log.Infof("cpu usage: container=%s err=%v", container, err)
			continue
		}

		metrics = append(
			metrics,
			metric.NewContainerGaugeData(container, "cores", numCores, "cpu core number for the containers", nil),
		)
		if !dataCache.ready {
			continue
		}
		metrics = append(
			metrics,
			metric.NewContainerGaugeData(container, "usr", dataCache.usrUtil, "cpu usr for the containers", nil),
			metric.NewContainerGaugeData(container, "sys", dataCache.sysUtil, "cpu sys for the containers", nil),
			metric.NewContainerGaugeData(container, "total", dataCache.totalUtil, "cpu total for the containers", nil),
		)
	}

	more, err := c.updateHostDataCache()
	if err != nil {
		log.Warnf("host cpu usage: %v", err)
	}

	return append(metrics, more...), nil
}
