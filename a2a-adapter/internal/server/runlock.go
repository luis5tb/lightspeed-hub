package server

import "sync"

// runLocks serializes approval/revision operations and execution-token
// reconciliation for a single run. The adapter is deployed with one replica;
// this is process-local coordination, not a distributed lock.
type runLocks struct {
	mu    sync.Mutex
	locks map[string]*runLockEntry
}

type runLockEntry struct {
	mu   sync.Mutex
	refs int
}

func newRunLocks() *runLocks {
	return &runLocks{locks: make(map[string]*runLockEntry)}
}

func (l *runLocks) Lock(key string) func() {
	entry := l.entry(key)
	entry.mu.Lock()
	return l.release(key, entry)
}

func (l *runLocks) TryLock(key string) (func(), bool) {
	entry := l.entry(key)
	if !entry.mu.TryLock() {
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.locks, key)
		}
		l.mu.Unlock()
		return nil, false
	}
	return l.release(key, entry), true
}

func (l *runLocks) entry(key string) *runLockEntry {
	l.mu.Lock()
	entry := l.locks[key]
	if entry == nil {
		entry = &runLockEntry{}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()
	return entry
}

func (l *runLocks) release(key string, entry *runLockEntry) func() {
	return func() {
		entry.mu.Unlock()
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.locks, key)
		}
		l.mu.Unlock()
	}
}
