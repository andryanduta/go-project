package util

import (
	"time"
)

type (
	Timer struct {
		InitRunTime time.Time
		LastRunTime time.Time
	}
)

func NewTimer() *Timer {
	return &Timer{}
}

func (t *Timer) StartTimer() {
	now := time.Now()

	t.InitRunTime = now
	t.LastRunTime = now
}

func (t *Timer) GetElapsedTime() float64 {
	if iz := t.LastRunTime.IsZero(); iz {
		panic("Timer has not been started yet")
	}

	lastRun := t.LastRunTime

	elTime := time.Since(lastRun).Seconds()
	t.LastRunTime = time.Now()

	return elTime
}
