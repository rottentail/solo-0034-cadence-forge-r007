package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

const (
	MaxUploadBytes      int64 = 64 << 20
	MinChunkBytes       int64 = 1 << 10
	MaxChunkBytes       int64 = 8 << 20
	MaxChunkCount             = 10000
	uploadMeta                = "meta.json"
	uploadData                = "data"
	uploadMarker              = "chunk-%05d"
	uploadMarkerPattern       = "chunk-*"
)

var (
	ErrUploadNotFound = errors.New("upload session not found")
	ErrUploadState    = errors.New("upload session state conflict")
	ErrChunkIndex     = errors.New("chunk index outside session range")
	ErrChunkDigest    = errors.New("chunk content digest mismatch")
	ErrChunkSize      = errors.New("chunk size does not match session layout")
	ErrSessionDigest  = errors.New("assembled content digest mismatch")
	ErrSessionLayout  = errors.New("session layout is incomplete")
)

var uploadIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// UploadMeta is the durable declaration of one resumable upload session.
// It only describes bytes that are being received; it never names a source
// identity. A session becomes media only after CompleteUpload proves the
// assembled content digest and the application probe succeeds.
type UploadMeta struct {
	ID            string    `json:"id"`
	Size          int64     `json:"size"`
	ChunkSize     int64     `json:"chunk_size"`
	ChunkCount    int       `json:"chunk_count"`
	ContentSHA256 string    `json:"content_sha256,omitempty"`
	ChunkSHA256   []string  `json:"chunk_sha256,omitempty"`
	Digest        string    `json:"digest,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	State         string    `json:"state"`
}

// UploadStatus is the externally observable, identity-free session view.
type UploadStatus struct {
	ID            string `json:"id"`
	Size          int64  `json:"size"`
	ChunkSize     int64  `json:"chunk_size"`
	ChunkCount    int    `json:"chunk_count"`
	Received      []int  `json:"received"`
	Missing       []int  `json:"missing"`
	State         string `json:"state"`
	ContentSHA256 string `json:"content_sha256,omitempty"`
	Digest        string `json:"digest,omitempty"`
}

func (s *Store) uploadDir(id string) (string, error) {
	if !uploadIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid upload id")
	}
	return filepath.Join(s.Root, "uploads", id), nil
}

func (s *Store) uploadLock(id string) *sync.Mutex {
	s.uploadGuard.Lock()
	defer s.uploadGuard.Unlock()
	mu, ok := s.uploadLocks[id]
	if !ok {
		mu = &sync.Mutex{}
		s.uploadLocks[id] = mu
	}
	return mu
}

func writeJSONAtomic(path string, value any) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".meta-*")
	if err != nil {
		return err
	}
	name := f.Name()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		_, err = f.Write(encoded)
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = f.Close()
	} else {
		f.Close()
	}
	if err != nil {
		os.Remove(name)
		return err
	}
	if err = os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return SyncDir(filepath.Dir(path))
}

func loadUploadMeta(dir string) (UploadMeta, error) {
	var meta UploadMeta
	data, err := os.ReadFile(filepath.Join(dir, uploadMeta))
	if err != nil {
		if os.IsNotExist(err) {
			return meta, ErrUploadNotFound
		}
		return meta, err
	}
	if err = json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

// CreateUpload reserves one sparse destination file of exactly the declared
// source size. The directory plus the data file together hold at most one
// copy of the source bytes plus one in-flight chunk; no second full copy is
// ever materialized.
func (s *Store) CreateUpload(meta UploadMeta) (UploadStatus, error) {
	if !uploadIDPattern.MatchString(meta.ID) {
		return UploadStatus{}, fmt.Errorf("invalid upload id")
	}
	if meta.Size < 1 || meta.Size > MaxUploadBytes {
		return UploadStatus{}, fmt.Errorf("upload size must be 1..%d bytes", MaxUploadBytes)
	}
	if meta.ChunkSize < MinChunkBytes || meta.ChunkSize > MaxChunkBytes {
		return UploadStatus{}, fmt.Errorf("chunk_size must be %d..%d bytes", MinChunkBytes, MaxChunkBytes)
	}
	count := int((meta.Size + meta.ChunkSize - 1) / meta.ChunkSize)
	if count < 1 || count > MaxChunkCount {
		return UploadStatus{}, fmt.Errorf("chunk count must be 1..%d", MaxChunkCount)
	}
	if meta.ContentSHA256 != "" && !digestPattern.MatchString(meta.ContentSHA256) {
		return UploadStatus{}, fmt.Errorf("invalid content digest")
	}
	if len(meta.ChunkSHA256) != 0 {
		if len(meta.ChunkSHA256) != count {
			return UploadStatus{}, fmt.Errorf("chunk digest manifest must cover all %d chunks", count)
		}
		for _, d := range meta.ChunkSHA256 {
			if !digestPattern.MatchString(d) {
				return UploadStatus{}, fmt.Errorf("invalid chunk digest in manifest")
			}
		}
	}
	dir, err := s.uploadDir(meta.ID)
	if err != nil {
		return UploadStatus{}, err
	}
	if _, err = os.Stat(dir); err == nil {
		return UploadStatus{}, fmt.Errorf("upload id already exists")
	} else if !os.IsNotExist(err) {
		return UploadStatus{}, err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return UploadStatus{}, err
	}
	now := time.Now().UTC()
	meta.ChunkCount = count
	meta.State = "open"
	meta.CreatedAt = now
	meta.UpdatedAt = now
	data, err := os.OpenFile(filepath.Join(dir, uploadData), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		os.RemoveAll(dir)
		return UploadStatus{}, err
	}
	// Truncate reserves logical size without allocating the full source on disk;
	// writes are bounded by declared chunk offsets and the final size check.
	if err = data.Truncate(meta.Size); err != nil {
		data.Close()
		os.RemoveAll(dir)
		return UploadStatus{}, err
	}
	if err = data.Sync(); err != nil {
		data.Close()
		os.RemoveAll(dir)
		return UploadStatus{}, err
	}
	if err = data.Close(); err != nil {
		os.RemoveAll(dir)
		return UploadStatus{}, err
	}
	if err = writeJSONAtomic(filepath.Join(dir, uploadMeta), meta); err != nil {
		os.RemoveAll(dir)
		return UploadStatus{}, err
	}
	return s.UploadStatus(meta.ID)
}

func receivedChunks(dir string) (map[int]bool, error) {
	entries, err := filepath.Glob(filepath.Join(dir, uploadMarkerPattern))
	if err != nil {
		return nil, err
	}
	received := map[int]bool{}
	for _, entry := range entries {
		name := filepath.Base(entry)
		var index int
		if _, err = fmt.Sscanf(name, uploadMarker, &index); err != nil {
			continue
		}
		received[index] = true
	}
	return received, nil
}

// UploadStatus reconstructs progress from durable marker files, so the view
// stays truthful across process restarts without trusting process memory.
func (s *Store) UploadStatus(id string) (UploadStatus, error) {
	dir, err := s.uploadDir(id)
	if err != nil {
		return UploadStatus{}, err
	}
	meta, err := loadUploadMeta(dir)
	if err != nil {
		return UploadStatus{}, err
	}
	receivedSet, err := receivedChunks(dir)
	if err != nil {
		return UploadStatus{}, err
	}
	received := make([]int, 0, len(receivedSet))
	for index := range receivedSet {
		if index >= 0 && index < meta.ChunkCount {
			received = append(received, index)
		}
	}
	sort.Ints(received)
	missing := []int{}
	for i := 0; i < meta.ChunkCount; i++ {
		if !receivedSet[i] {
			missing = append(missing, i)
		}
	}
	return UploadStatus{ID: meta.ID, Size: meta.Size, ChunkSize: meta.ChunkSize, ChunkCount: meta.ChunkCount, Received: received, Missing: missing, State: meta.State, ContentSHA256: meta.ContentSHA256, Digest: meta.Digest}, nil
}

func expectedChunkSize(meta UploadMeta, index int) int64 {
	start := int64(index) * meta.ChunkSize
	size := meta.ChunkSize
	if remaining := meta.Size - start; remaining < size {
		size = remaining
	}
	return size
}

// PutChunk streams one chunk into its declared offset. The destination file
// is preallocated to the full size, so a wrong offset can never grow the
// session beyond one source-size budget. Concurrent uploads of the same
// chunk are allowed but serialized: identical retries converge, divergent
// bytes for one identity are rejected with 409.
func (s *Store) PutChunk(id string, index int, body io.Reader) (UploadStatus, error) {
	dir, err := s.uploadDir(id)
	if err != nil {
		return UploadStatus{}, err
	}
	mu := s.uploadLock(id)
	mu.Lock()
	defer mu.Unlock()

	meta, err := loadUploadMeta(dir)
	if err != nil {
		return UploadStatus{}, err
	}
	if meta.State != "open" {
		return UploadStatus{}, fmt.Errorf("%w: session is %s", ErrUploadState, meta.State)
	}
	if index < 0 || index >= meta.ChunkCount {
		return UploadStatus{}, fmt.Errorf("%w: %d", ErrChunkIndex, index)
	}
	want := expectedChunkSize(meta, index)
	tmp, err := os.CreateTemp(dir, ".part-*")
	if err != nil {
		return UploadStatus{}, err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(body, want+1))
	if err != nil {
		tmp.Close()
		return UploadStatus{}, err
	}
	if n != want {
		tmp.Close()
		return UploadStatus{}, fmt.Errorf("%w: got %d want %d", ErrChunkSize, n, want)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if index < len(meta.ChunkSHA256) && digest != meta.ChunkSHA256[index] {
		tmp.Close()
		return UploadStatus{}, fmt.Errorf("%w: chunk %d", ErrChunkDigest, index)
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return UploadStatus{}, err
	}
	if err = tmp.Close(); err != nil {
		return UploadStatus{}, err
	}
	marker := filepath.Join(dir, fmt.Sprintf(uploadMarker, index))
	if _, err = os.Stat(marker); err == nil {
		recorded, rerr := os.ReadFile(marker)
		if rerr == nil {
			var claim struct {
				SHA256 string `json:"sha256"`
			}
			_ = json.Unmarshal(recorded, &claim)
			if claim.SHA256 != "" && claim.SHA256 != digest {
				return UploadStatus{}, fmt.Errorf("%w: chunk %d already holds different bytes", ErrUploadState, index)
			}
		}
		// Idempotent replay: rewrite the data region unconditionally. A crash
		// between marker durability and the data write could otherwise leave the
		// shared data file short or stale while the marker claims completion.
		if err = writeChunkData(dir, meta, index, tmp.Name()); err != nil {
			return UploadStatus{}, err
		}
		return s.UploadStatus(id)
	} else if !os.IsNotExist(err) {
		return UploadStatus{}, err
	}
	// Marker first makes the chunk durable and replay-detectable; the data
	// region is then written at its fixed offset. Replays always rewrite that
	// region, so a crash in between self-heals on the next attempt.
	if err = writeJSONAtomic(marker, map[string]string{"sha256": digest}); err != nil {
		return UploadStatus{}, err
	}
	if err = writeChunkData(dir, meta, index, tmp.Name()); err != nil {
		return UploadStatus{}, err
	}
	meta.UpdatedAt = time.Now().UTC()
	if err = writeJSONAtomic(filepath.Join(dir, uploadMeta), meta); err != nil {
		return UploadStatus{}, err
	}
	return s.UploadStatus(id)
}

func writeChunkData(dir string, meta UploadMeta, index int, part string) error {
	data, err := os.OpenFile(filepath.Join(dir, uploadData), os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer data.Close()
	if _, err = data.Seek(int64(index)*meta.ChunkSize, io.SeekStart); err != nil {
		return err
	}
	f, err := os.Open(part)
	if err != nil {
		return err
	}
	if _, err = io.Copy(data, f); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return data.Sync()
}

// CompleteUpload requires every marker, verifies the assembled file size and
// whole-content digest, then hard-links the single data file into the
// content-addressed object store. Hard links share inodes, so promotion does
// not consume a second full-copy budget and the session stays reclaimable.
// It is idempotent across retries: once the object is promoted, a repeated
// call returns the same digest so a later probe/journal failure can be
// retried without re-uploading any bytes.
func (s *Store) CompleteUpload(id string) (string, int64, error) {
	dir, err := s.uploadDir(id)
	if err != nil {
		return "", 0, err
	}
	mu := s.uploadLock(id)
	mu.Lock()
	defer mu.Unlock()

	meta, err := loadUploadMeta(dir)
	if err != nil {
		return "", 0, err
	}
	if meta.State == "completed" && meta.Digest != "" {
		if dest, perr := s.Object(meta.Digest); perr == nil {
			if info, serr := os.Stat(dest); serr == nil {
				return meta.Digest, info.Size(), nil
			}
		}
		// Object missing despite the completed marker: fall through to recompute
		// and relink rather than declaring the session unusable.
	}
	if meta.State != "open" && meta.State != "completed" {
		return "", 0, fmt.Errorf("%w: session is %s", ErrUploadState, meta.State)
	}
	received, err := receivedChunks(dir)
	if err != nil {
		return "", 0, err
	}
	for i := 0; i < meta.ChunkCount; i++ {
		if !received[i] {
			return "", 0, fmt.Errorf("%w: chunk %d missing", ErrSessionLayout, i)
		}
	}
	dataPath := filepath.Join(dir, uploadData)
	info, err := os.Stat(dataPath)
	if err != nil {
		return "", 0, err
	}
	if info.Size() != meta.Size {
		return "", 0, fmt.Errorf("%w: assembled size %d want %d", ErrSessionLayout, info.Size(), meta.Size)
	}
	f, err := os.Open(dataPath)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		return "", 0, err
	}
	if n != meta.Size {
		return "", 0, fmt.Errorf("%w: assembled size %d want %d", ErrSessionLayout, n, meta.Size)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if meta.ContentSHA256 != "" && digest != meta.ContentSHA256 {
		return "", 0, fmt.Errorf("%w: got %s want %s", ErrSessionDigest, digest, meta.ContentSHA256)
	}
	if meta.Digest != "" && meta.Digest != digest {
		return "", 0, fmt.Errorf("%w: promoted digest %s no longer matches %s", ErrSessionDigest, meta.Digest, digest)
	}
	dest, err := s.Object(digest)
	if err != nil {
		return "", 0, err
	}
	if err = os.Link(dataPath, dest); err != nil && !os.IsExist(err) {
		return "", 0, err
	}
	if err = SyncDir(filepath.Dir(dest)); err != nil {
		return "", 0, err
	}
	meta.State = "completed"
	meta.Digest = digest
	meta.UpdatedAt = time.Now().UTC()
	if err = writeJSONAtomic(filepath.Join(dir, uploadMeta), meta); err != nil {
		return "", 0, err
	}
	return digest, n, nil
}

// AbortUpload removes the whole session directory. In-flight chunk writers
// fail on their next filesystem operation; partial markers can never name a
// source because only CompleteUpload promotes into the object store.
func (s *Store) AbortUpload(id string) error {
	dir, err := s.uploadDir(id)
	if err != nil {
		return err
	}
	mu := s.uploadLock(id)
	mu.Lock()
	defer mu.Unlock()
	if _, err = os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return ErrUploadNotFound
		}
		return err
	}
	return os.RemoveAll(dir)
}

// SweepUploads reclaims sessions that predate the retention horizon and have
// not seen progress, bounding temporary space to one source-size file per
// live session. Completed sessions are swept as well: their content already
// lives in the content-addressed object store.
func (s *Store) SweepUploads(ttl time.Duration) (int, error) {
	base := filepath.Join(s.Root, "uploads")
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	cutoff := time.Now().Add(-ttl)
	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() || !uploadIDPattern.MatchString(entry.Name()) {
			continue
		}
		dir := filepath.Join(base, entry.Name())
		meta, err := loadUploadMeta(dir)
		if err == nil {
			if meta.UpdatedAt.After(cutoff) {
				continue
			}
		} else {
			// Unreadable or partially created directories are removed only past
			// the horizon; fresh crash debris is left untouched for diagnosis.
			info, statErr := os.Stat(dir)
			if statErr != nil || info.ModTime().After(cutoff) {
				continue
			}
		}
		if err = os.RemoveAll(dir); err == nil {
			removed++
		}
	}
	if removed > 0 {
		if err = SyncDir(base); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// ChunkDigest is a convenience for clients that want to prove one chunk body
// before retrying; the server recomputes the digest independently.
func ChunkDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
