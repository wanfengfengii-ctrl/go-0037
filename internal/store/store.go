// Package store implements the embedded, transactional persistence layer for
// the cold-chain handoff ledger using bbolt, a pure-Go key-value store with
// ACID transactions. Every record related to a single event submission (the
// event envelope, its idempotency index, its terminal-sequence index and the
// affected box projection) is written in a single transaction, so a crash or
// injected fault can never leave half an event, an occupied idempotency key
// without a result, or a partially updated projection.
//
// The store deliberately knows nothing about domain rules or causal
// dependencies: it is a typed, transactional record store over which the
// coordinator implements the ledger semantics.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Bucket names.
const (
	bucketEvents   = "events"
	bucketIdem     = "idempotency"
	bucketTerminal = "terminal_seq"
	bucketBoxes    = "boxes"
	bucketPending  = "pending"
	bucketRejected = "rejected"
	bucketMeta     = "meta"
)

var requiredBuckets = [...]string{
	bucketEvents,
	bucketIdem,
	bucketTerminal,
	bucketBoxes,
	bucketPending,
	bucketRejected,
	bucketMeta,
}

// Status is the processing state of a recorded event.
type Status string

const (
	StatusPending  Status = "pending"
	StatusAccepted Status = "accepted"
	StatusRejected Status = "rejected"
)

// Record is the persisted representation of an event and its outcome.
type Record struct {
	EventID            string          `json:"event_id"`
	IdempotencyKey     string          `json:"idempotency_key"`
	TerminalID         string          `json:"terminal_id"`
	TerminalSequence   int64           `json:"terminal_sequence"`
	BoxID              string          `json:"box_id"`
	EventType          string          `json:"event_type"`
	Role               string          `json:"role"`
	OccurredAt         time.Time       `json:"occurred_at"`
	ReceivedAt         time.Time       `json:"received_at"`
	PredecessorEventID string          `json:"predecessor_event_id,omitempty"`
	PayloadHash        string          `json:"payload_hash"`
	Envelope           json.RawMessage `json:"envelope"`
	Status             Status          `json:"status"`
	Revision           int64           `json:"revision,omitempty"`
	NewState           string          `json:"new_state,omitempty"`
	Reason             string          `json:"reason,omitempty"`
	Code               string          `json:"code,omitempty"`
}

// IdemIndex maps an idempotency key (or terminal seq) to an event.
type IdemIndex struct {
	EventID     string `json:"event_id"`
	PayloadHash string `json:"payload_hash"`
}

// PendingRecord captures why an event is awaiting dependencies.
type PendingRecord struct {
	EventID            string   `json:"event_id"`
	BoxID              string   `json:"box_id"`
	MissingPredecessor string   `json:"missing_predecessor,omitempty"`
	MissingTerminalSeq int64    `json:"missing_terminal_seq,omitempty"`
	MissingTerminalID  string   `json:"missing_terminal_id,omitempty"`
	Reasons            []string `json:"reasons,omitempty"`
}

// Store is a transactional bbolt-backed record store.
type Store struct {
	db     *bolt.DB
	mu     sync.RWMutex // serialises Update/View callers against Close
	closed bool
}

// Open opens (or creates) a store at path. The directory is created if needed.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: create data dir: %w", err)
		}
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range requiredBuckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// OpenReadOnly opens an existing store without creating a directory, database
// file, or missing buckets.
func OpenReadOnly(path string) (*Store, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("store: open read-only %s: %w", path, err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("store: open read-only %s: %w", path, err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		for _, b := range requiredBuckets {
			if tx.Bucket([]byte(b)) == nil {
				return fmt.Errorf("required bucket %q is missing", b)
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: validate read-only %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// Path returns the database file path.
func (s *Store) Path() string { return s.db.Path() }

// Update runs fn inside a read-write transaction. If fn returns an error the
// transaction is rolled back and every write performed within it is discarded.
func (s *Store) Update(fn func(*Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// View runs fn inside a read-only transaction.
func (s *Store) View(fn func(*Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return s.db.View(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// ErrClosed is returned when the store is used after Close.
var ErrClosed = errors.New("store: closed")

// Tx wraps a bbolt transaction and exposes typed accessors.
type Tx struct {
	tx *bolt.Tx
}

func (t *Tx) bucket(name string) *bolt.Bucket {
	return t.tx.Bucket([]byte(name))
}

// PutRecord writes an event record, keyed by event id.
func (t *Tx) PutRecord(r *Record) error {
	b := t.bucket(bucketEvents)
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return b.Put([]byte(r.EventID), data)
}

// GetRecord reads an event record by event id. Returns nil, nil if absent.
func (t *Tx) GetRecord(eventID string) (*Record, error) {
	v := t.bucket(bucketEvents).Get([]byte(eventID))
	if v == nil {
		return nil, nil
	}
	var r Record
	if err := json.Unmarshal(v, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// PutIdem writes an idempotency-key index entry.
func (t *Tx) PutIdem(key string, idx *IdemIndex) error {
	data, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	return t.bucket(bucketIdem).Put([]byte(key), data)
}

// GetIdem reads an idempotency-key index entry.
func (t *Tx) GetIdem(key string) (*IdemIndex, error) {
	v := t.bucket(bucketIdem).Get([]byte(key))
	if v == nil {
		return nil, nil
	}
	var idx IdemIndex
	if err := json.Unmarshal(v, &idx); err != nil {
		return nil, err
	}
	return &idx, nil
}

// PutTerminal writes a terminal-sequence index entry.
func (t *Tx) PutTerminal(terminalID string, seq int64, idx *IdemIndex) error {
	data, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	return t.bucket(bucketTerminal).Put(TerminalKey(terminalID, seq), data)
}

// GetTerminal reads a terminal-sequence index entry.
func (t *Tx) GetTerminal(terminalID string, seq int64) (*IdemIndex, error) {
	v := t.bucket(bucketTerminal).Get(TerminalKey(terminalID, seq))
	if v == nil {
		return nil, nil
	}
	var idx IdemIndex
	if err := json.Unmarshal(v, &idx); err != nil {
		return nil, err
	}
	return &idx, nil
}

// PutBox writes a box projection.
func (t *Tx) PutBox(boxID string, data []byte) error {
	return t.bucket(bucketBoxes).Put([]byte(boxID), data)
}

// GetBox reads a box projection.
func (t *Tx) GetBox(boxID string) ([]byte, error) {
	v := t.bucket(bucketBoxes).Get([]byte(boxID))
	if v == nil {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// DeleteBox removes a box projection.
func (t *Tx) DeleteBox(boxID string) error {
	return t.bucket(bucketBoxes).Delete([]byte(boxID))
}

// ForEachBox iterates over all box projections.
func (t *Tx) ForEachBox(fn func(boxID string, data []byte) error) error {
	return t.bucket(bucketBoxes).ForEach(func(k, v []byte) error {
		return fn(string(k), v)
	})
}

// PutPending writes a pending record.
func (t *Tx) PutPending(p *PendingRecord) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return t.bucket(bucketPending).Put([]byte(p.EventID), data)
}

// DeletePending removes a pending record.
func (t *Tx) DeletePending(eventID string) error {
	return t.bucket(bucketPending).Delete([]byte(eventID))
}

// GetPending reads a pending record.
func (t *Tx) GetPending(eventID string) (*PendingRecord, error) {
	v := t.bucket(bucketPending).Get([]byte(eventID))
	if v == nil {
		return nil, nil
	}
	var p PendingRecord
	if err := json.Unmarshal(v, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// ForEachPending iterates over all pending records.
func (t *Tx) ForEachPending(fn func(*PendingRecord) error) error {
	return t.bucket(bucketPending).ForEach(func(_, v []byte) error {
		var p PendingRecord
		if err := json.Unmarshal(v, &p); err != nil {
			return err
		}
		return fn(&p)
	})
}

// CountPending returns the number of pending records.
func (t *Tx) CountPending() (int, error) {
	n := 0
	err := t.bucket(bucketPending).ForEach(func(_, _ []byte) error {
		n++
		return nil
	})
	return n, err
}

// ForEachRecord iterates over all event records.
func (t *Tx) ForEachRecord(fn func(*Record) error) error {
	return t.bucket(bucketEvents).ForEach(func(_, v []byte) error {
		var r Record
		if err := json.Unmarshal(v, &r); err != nil {
			return err
		}
		return fn(&r)
	})
}

// ForEachRejected iterates over all rejected records.
func (t *Tx) ForEachRejected(fn func(*Record) error) error {
	return t.bucket(bucketRejected).ForEach(func(_, v []byte) error {
		var r Record
		if err := json.Unmarshal(v, &r); err != nil {
			return err
		}
		return fn(&r)
	})
}

// PutRejected writes a rejected record.
func (t *Tx) PutRejected(r *Record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return t.bucket(bucketRejected).Put([]byte(r.EventID), data)
}

// DeleteRejected removes a rejected record.
func (t *Tx) DeleteRejected(eventID string) error {
	return t.bucket(bucketRejected).Delete([]byte(eventID))
}

// ClearBoxes deletes every box projection (used by Rebuild).
func (t *Tx) ClearBoxes() error {
	return t.bucket(bucketBoxes).ForEach(func(k, _ []byte) error {
		return t.bucket(bucketBoxes).Delete(k)
	})
}

// SetMeta / GetMeta store small named values (e.g. ledger open time).
func (t *Tx) SetMeta(key string, val []byte) error {
	return t.bucket(bucketMeta).Put([]byte(key), val)
}

func (t *Tx) GetMeta(key string) ([]byte, error) {
	v := t.bucket(bucketMeta).Get([]byte(key))
	if v == nil {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}
