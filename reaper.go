package fscache

import "time"

// Reaper is used to control when streams expire from the cache.
// It is called once right after loading, and then it is run
// again after every Next() period of time.
type Reaper interface {
	// Returns the amount of time to wait before the next scheduled Reaping.
	Next() time.Duration

	// Given a key and the last r/w times of a file, return true
	// to remove the file from the cache, false to keep it.
	Reap(key string, lastRead, lastWrite time.Time) bool
}

// NewReaper returns a simple reaper which runs every "period"
// and reaps files which last reads are older than "expiry".
// See NewLastReadReaper.
func NewReaper(expiry, period time.Duration) Reaper {
	return NewLastReadReaper(expiry, period)
}

// NewLastReadReaper returns a simple reaper which runs every "period"
// and reaps files which last reads are older than "expiry".
func NewLastReadReaper(expiry, period time.Duration) Reaper {
	return &reaper{
		expiry:          expiry,
		period:          period,
		reapOnLastWrite: false,
	}
}

// NewLastWriteReaper returns a simple reaper which runs every "period"
// and reaps files which last writes are older than "expiry".
func NewLastWriteReaper(expiry, period time.Duration) Reaper {
	return &reaper{
		expiry:          expiry,
		period:          period,
		reapOnLastWrite: true,
	}
}

type reaper struct {
	period          time.Duration
	expiry          time.Duration
	reapOnLastWrite bool
}

func (g *reaper) Next() time.Duration {
	return g.period
}

func (g *reaper) Reap(key string, lastRead, lastWrite time.Time) bool {
	if g.reapOnLastWrite {
		return lastWrite.Before(time.Now().Add(-g.expiry))
	}
	return lastRead.Before(time.Now().Add(-g.expiry))
}
