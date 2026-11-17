package service

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/config"
	"cadenceforge/internal/journal"
	"cadenceforge/internal/media"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
)

type Service struct {
	Store    *journal.Store
	Blobs    *blob.Store
	Probe    media.Prober
	Config   config.Config
	mutation sync.Mutex
	wake     chan struct{}
	activeMu sync.Mutex
	active   map[string]context.CancelFunc
}

func New(c config.Config) (*Service, error) {
	st, err := journal.Open(c.Root)
	if err != nil {
		return nil, err
	}
	blobs, err := blob.Open(c.Root)
	if err != nil {
		st.Close()
		return nil, err
	}
	s := &Service{Store: st, Blobs: blobs, Probe: media.Prober{Binary: c.FFprobe}, Config: c, wake: make(chan struct{}, 1), active: map[string]context.CancelFunc{}}
	if removed, err := blobs.SweepUploads(c.UploadTTL); err != nil {
		st.Close()
		return nil, err
	} else if removed > 0 {
		slog.Info("expired upload sessions reclaimed", "count", removed)
	}
	if err = st.Recover(); err != nil {
		st.Close()
		return nil, err
	}
	return s, nil
}
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("entropy unavailable: %w", err))
	}
	return hex.EncodeToString(b[:])
}
func hash(value any) string {
	b, _ := json.Marshal(value)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (s *Service) Notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
