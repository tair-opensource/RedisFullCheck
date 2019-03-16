package metric

import (
	"fmt"
	"sync/atomic"
	"full_check/common"
)

type CounterStat struct {
	Total int64 `json:"total"`
	Speed int64 `json:"speed"`
}

type AtomicSpeedCounter struct {
	total       int64
	intervalSum int64
	lastSpeed   int64
}

func (p *AtomicSpeedCounter) Inc(i int) {
	atomic.AddInt64(&p.total, int64(i))
	atomic.AddInt64(&p.intervalSum, int64(i))
}

func (p *AtomicSpeedCounter) Rotate() {
	old := atomic.SwapInt64(&p.intervalSum, 0)
	p.lastSpeed = (old + common.StatRollFrequency - 1) / common.StatRollFrequency
}

func (p *AtomicSpeedCounter) Reset() {
	p.total = 0
	p.intervalSum = 0
	p.lastSpeed = 0
}

func (p *AtomicSpeedCounter) Total() int64 {
	return p.total
}

func (p *AtomicSpeedCounter) Speed() int64 {
	return p.lastSpeed
}

func (p *AtomicSpeedCounter) String() string {
	return fmt.Sprintf("total:%d,speed:%d", p.total, p.lastSpeed)
}

func (p *AtomicSpeedCounter) Json() *CounterStat {
	return &CounterStat{Total: p.total, Speed: p.lastSpeed}
}
