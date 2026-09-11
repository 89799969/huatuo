// Copyright 2026 The HuaTuo Authors
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
	"testing"
	"time"

	"github.com/ccfos/huatuo/internal/cgroups"
	"github.com/ccfos/huatuo/internal/cgroups/stats"
)

type cpuUsageCgroup struct {
	cgroups.Cgroup
	usage stats.CpuUsage
}

func (c *cpuUsageCgroup) CpuUsage(string) (*stats.CpuUsage, error) {
	return &c.usage, nil
}

func TestComputeCPUUtil(t *testing.T) {
	tests := []struct {
		name      string
		prev      stats.CpuUsage
		curr      stats.CpuUsage
		elapsed   time.Duration
		numCores  float64
		wantTotal float64
		wantUsr   float64
		wantSys   float64
		wantOK    bool
	}{
		{
			name:      "full core over one second",
			prev:      stats.CpuUsage{Usage: 0, User: 0, System: 0},
			curr:      stats.CpuUsage{Usage: 1_000_000, User: 800_000, System: 200_000},
			elapsed:   time.Second,
			numCores:  1,
			wantTotal: 100,
			wantUsr:   80,
			wantSys:   20,
			wantOK:    true,
		},
		{
			name:      "half of two cores",
			prev:      stats.CpuUsage{Usage: 10, User: 5, System: 5},
			curr:      stats.CpuUsage{Usage: 1_000_010, User: 500_005, System: 500_005},
			elapsed:   time.Second,
			numCores:  2,
			wantTotal: 50,
			wantUsr:   25,
			wantSys:   25,
			wantOK:    true,
		},
		{
			name:     "first sample has no elapsed window",
			prev:     stats.CpuUsage{},
			curr:     stats.CpuUsage{Usage: 3_600_000_000, User: 2_000_000_000, System: 1_000_000_000},
			elapsed:  0,
			numCores: 1,
		},
		{
			name:     "counter regression",
			prev:     stats.CpuUsage{Usage: 10, User: 6, System: 4},
			curr:     stats.CpuUsage{Usage: 9, User: 6, System: 4},
			elapsed:  time.Second,
			numCores: 1,
		},
		{
			name:     "usage exceeds wall-clock capacity",
			prev:     stats.CpuUsage{},
			curr:     stats.CpuUsage{Usage: 2_000_000, User: 1_000_000, System: 1_000_000},
			elapsed:  time.Second,
			numCores: 1,
		},
		{
			name:     "zero cores",
			prev:     stats.CpuUsage{},
			curr:     stats.CpuUsage{Usage: 1_000_000},
			elapsed:  time.Second,
			numCores: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, usr, sys, ok := computeCPUUtil(tt.prev, tt.curr, tt.elapsed, tt.numCores)
			if ok != tt.wantOK {
				t.Fatalf("computeCPUUtil() ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if total != tt.wantTotal || usr != tt.wantUsr || sys != tt.wantSys {
				t.Fatalf(
					"computeCPUUtil() = (%v, %v, %v), want (%v, %v, %v)",
					total, usr, sys, tt.wantTotal, tt.wantUsr, tt.wantSys,
				)
			}
		})
	}
}

func TestCPUUtilCollectorUpdateDataCacheFirstSampleIsBaselineOnly(t *testing.T) {
	// A long-lived busy container reports a large cumulative Usage. Treating
	// that as a delta over wall-clock-since-zero yields ~0% utilization.
	cache := cpuUtilStat{}
	collector := cpuUtilCollector{
		cgroup: &cpuUsageCgroup{usage: stats.CpuUsage{Usage: 3_600_000_000, User: 2_000_000_000, System: 1_000_000_000}},
	}

	if err := collector.updateDataCache(&cache, nil, 1); err != nil {
		t.Fatalf("updateDataCache() error = %v", err)
	}
	if cache.ready {
		t.Fatal("first sample must not publish utilization")
	}
	if cache.totalUtil != 0 || cache.usrUtil != 0 || cache.sysUtil != 0 {
		t.Fatalf(
			"first sample published utilization: total=%v user=%v system=%v",
			cache.totalUtil, cache.usrUtil, cache.sysUtil,
		)
	}
	if cache.lastTimestamp.IsZero() {
		t.Fatal("first sample must record a baseline timestamp")
	}
	if cache.lastUsage.Usage != 3_600_000_000 {
		t.Fatalf("baseline usage = %v, want 3600000000", cache.lastUsage.Usage)
	}
}

func TestCPUUtilCollectorUpdateDataCachePublishesAfterBaseline(t *testing.T) {
	lastTimestamp := time.Now().Add(-2 * time.Second)
	cache := cpuUtilStat{
		lastUsage:     stats.CpuUsage{Usage: 0, User: 0, System: 0},
		lastTimestamp: lastTimestamp,
	}
	// 2s of CPU on one core over a ~2s window is 100%; the exact wall
	// clock is machine-dependent, so only assert a non-zero published rate.
	collector := cpuUtilCollector{
		cgroup: &cpuUsageCgroup{usage: stats.CpuUsage{Usage: 2_000_000, User: 1_600_000, System: 400_000}},
	}

	if err := collector.updateDataCache(&cache, nil, 1); err != nil {
		t.Fatalf("updateDataCache() error = %v", err)
	}
	if !cache.ready {
		t.Fatal("second sample must publish utilization")
	}
	if cache.totalUtil <= 0 || cache.totalUtil > 100 {
		t.Fatalf("total utilization = %v, want in (0, 100]", cache.totalUtil)
	}
	if cache.usrUtil <= 0 || cache.sysUtil <= 0 {
		t.Fatalf("usr/sys utilization = %v/%v, want both positive", cache.usrUtil, cache.sysUtil)
	}
}

func TestCPUUtilCollectorUpdateDataCacheCounterRegression(t *testing.T) {
	tests := []struct {
		name    string
		current stats.CpuUsage
	}{
		{
			name:    "total usage regresses",
			current: stats.CpuUsage{Usage: 9, User: 6, System: 4},
		},
		{
			name:    "user usage regresses",
			current: stats.CpuUsage{Usage: 10, User: 5, System: 4},
		},
		{
			name:    "system usage regresses",
			current: stats.CpuUsage{Usage: 10, User: 6, System: 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastTimestamp := time.Now().Add(-2 * time.Second)
			cache := cpuUtilStat{
				lastUsage:     stats.CpuUsage{Usage: 10, User: 6, System: 4},
				lastTimestamp: lastTimestamp,
				totalUtil:     11,
				usrUtil:       22,
				sysUtil:       33,
			}
			collector := cpuUtilCollector{
				cgroup: &cpuUsageCgroup{usage: tt.current},
			}

			if err := collector.updateDataCache(&cache, nil, 1); err != nil {
				t.Fatalf("updateDataCache() error = %v", err)
			}
			if cache.lastUsage != tt.current {
				t.Fatalf("last usage = %+v, want %+v", cache.lastUsage, tt.current)
			}
			if !cache.lastTimestamp.After(lastTimestamp) {
				t.Fatalf("last timestamp = %v, want after %v", cache.lastTimestamp, lastTimestamp)
			}
			if cache.totalUtil != 11 || cache.usrUtil != 22 || cache.sysUtil != 33 {
				t.Fatalf(
					"utilization changed after counter regression: total=%v user=%v system=%v",
					cache.totalUtil,
					cache.usrUtil,
					cache.sysUtil,
				)
			}
		})
	}
}
