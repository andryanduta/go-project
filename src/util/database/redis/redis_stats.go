package redis

import (
	"sync"
)

type (
	StatsCollectorConfig struct {
		Apps    string
		Enabled bool
	}
	StatsCollector struct {
		mu      *sync.Mutex
		Counter map[string]uint64
	}
)

var (
	statsClConfig StatsCollectorConfig = StatsCollectorConfig{Apps: "undefined", Enabled: false}
	sc            StatsCollector
)

func NewStatsCollector(apps string) {
	statsClConfig = StatsCollectorConfig{Apps: apps, Enabled: true}
	sc = StatsCollector{
		Counter: make(map[string]uint64),
		mu:      &sync.Mutex{},
	}
}

func SCIncrement(clientName string) {
	if !statsClConfig.Enabled {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.Counter[clientName]++
}

func SCGet(clientName string) uint64 {
	if !statsClConfig.Enabled {
		return 0
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.Counter[clientName]
}

func SCGetAll() (StatsCollectorConfig, StatsCollector) {
	if !statsClConfig.Enabled {
		return statsClConfig, sc
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	return statsClConfig, sc
}

func SCGetAllAndReset() (StatsCollectorConfig, StatsCollector) {
	if !statsClConfig.Enabled {
		return statsClConfig, sc
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	var scCopy StatsCollector
	scCopy = sc

	sc.Counter = make(map[string]uint64)

	return statsClConfig, scCopy
}
