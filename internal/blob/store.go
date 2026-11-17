package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var idPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type Store struct {
	Root string

	uploadGuard sync.Mutex
	uploadLocks map[string]*sync.Mutex
}

func Open(root string) (*Store, error) {
	for _, dir := range []string{"objects", "staging", "renders", "uploads"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			return nil, err
		}
	}
	return &Store{Root: root, uploadLocks: map[string]*sync.Mutex{}}, nil
}
func (s *Store) Object(digest string) (string, error) {
	if !digestPattern.MatchString(digest) {
		return "", fmt.Errorf("invalid content digest")
	}
	return filepath.Join(s.Root, "objects", digest), nil
}
func (s *Store) Receive(r io.Reader, limit int64) (string, int64, error) {
	f, err := os.CreateTemp(filepath.Join(s.Root, "staging"), "upload-*")
	if err != nil {
		return "", 0, err
	}
	defer os.Remove(f.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r, limit+1))
	if err != nil {
		f.Close()
		return "", n, err
	}
	if n == 0 || n > limit {
		f.Close()
		return "", n, fmt.Errorf("upload must contain 1..%d bytes", limit)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return "", n, err
	}
	if err = f.Close(); err != nil {
		return "", n, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	dest, _ := s.Object(digest)
	if err = os.Link(f.Name(), dest); err != nil && !os.IsExist(err) {
		return "", n, err
	}
	if err = SyncDir(filepath.Dir(dest)); err != nil {
		return "", n, err
	}
	return digest, n, nil
}
func (s *Store) Staging(id string, attempt int) (string, error) {
	if !idPattern.MatchString(id) {
		return "", fmt.Errorf("invalid render id")
	}
	return os.MkdirTemp(filepath.Join(s.Root, "staging"), fmt.Sprintf("render-%s-%d-", id, attempt))
}
func (s *Store) RenderDir(id string, attempt int) (string, error) {
	if !idPattern.MatchString(id) {
		return "", fmt.Errorf("invalid render id")
	}
	return filepath.Join(s.Root, "renders", fmt.Sprintf("%s-%d", id, attempt)), nil
}
func SyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (s *Store) Promote(stage, id string, attempt int) error {
	entries, err := os.ReadDir(stage)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			return fmt.Errorf("unexpected output directory")
		}
		f, err := os.OpenFile(filepath.Join(stage, e.Name()), os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	if err = SyncDir(stage); err != nil {
		return err
	}
	dest, err := s.RenderDir(id, attempt)
	if err != nil {
		return err
	}
	if err = os.Rename(stage, dest); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(dest))
}
