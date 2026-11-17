package service

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/domain"
	"context"
	"errors"
	"io"
	"strings"
	"time"
)

// CreateUploadRequest is the byte-layout declaration of a resumable upload.
// It carries no source identity: the identity only exists once the assembled
// content digest is proven and the media probe is registered.
type CreateUploadRequest struct {
	Name          string   `json:"name"`
	Size          int64    `json:"size"`
	ChunkSize     int64    `json:"chunk_size"`
	ContentSHA256 string   `json:"content_sha256,omitempty"`
	ChunkSHA256   []string `json:"chunk_sha256,omitempty"`
}

func validateCreateUpload(input CreateUploadRequest) error {
	if strings.TrimSpace(input.Name) == "" || len(input.Name) > 160 {
		return domain.Invalid("source name must contain 1..160 bytes")
	}
	if input.Size < 1 || input.Size > blob.MaxUploadBytes {
		return domain.Invalid("upload size must be 1..%d bytes", blob.MaxUploadBytes)
	}
	if input.ChunkSize < blob.MinChunkBytes || input.ChunkSize > blob.MaxChunkBytes {
		return domain.Invalid("chunk_size must be %d..%d bytes", blob.MinChunkBytes, blob.MaxChunkBytes)
	}
	count := int((input.Size + input.ChunkSize - 1) / input.ChunkSize)
	if count > blob.MaxChunkCount {
		return domain.Invalid("upload would require more than %d chunks", blob.MaxChunkCount)
	}
	if len(input.ChunkSHA256) != 0 && len(input.ChunkSHA256) != count {
		return domain.Invalid("chunk digest manifest must cover all %d chunks", count)
	}
	return nil
}

func mapUploadError(id string, err error) error {
	switch {
	case errors.Is(err, blob.ErrUploadNotFound):
		return domain.Missing("upload", id)
	case errors.Is(err, blob.ErrUploadState):
		return domain.Conflict(err.Error())
	case errors.Is(err, blob.ErrChunkIndex), errors.Is(err, blob.ErrChunkSize), errors.Is(err, blob.ErrChunkDigest), errors.Is(err, blob.ErrSessionDigest), errors.Is(err, blob.ErrSessionLayout):
		return domain.Invalid("%s", err.Error())
	default:
		return err
	}
}

// CreateUpload opens an identity-free resumable session. Editors cannot see
// the session anywhere: it is not in the journal state and not an object.
func (s *Service) CreateUpload(input CreateUploadRequest) (blob.UploadStatus, error) {
	if err := validateCreateUpload(input); err != nil {
		return blob.UploadStatus{}, err
	}
	meta := blob.UploadMeta{
		ID:            ID(),
		Size:          input.Size,
		ChunkSize:     input.ChunkSize,
		ContentSHA256: input.ContentSHA256,
		ChunkSHA256:   append([]string(nil), input.ChunkSHA256...),
	}
	status, err := s.Blobs.CreateUpload(meta)
	if err != nil {
		return blob.UploadStatus{}, domain.Invalid("%s", err.Error())
	}
	return status, nil
}

func (s *Service) UploadStatus(id string) (blob.UploadStatus, error) {
	status, err := s.Blobs.UploadStatus(id)
	if err != nil {
		return blob.UploadStatus{}, mapUploadError(id, err)
	}
	return status, nil
}

// PutChunk accepts one declared chunk. Same-index concurrent retries are
// serialized at the blob layer; identical bytes converge, divergent bytes
// for the same session id are rejected so identities can never be mixed.
func (s *Service) PutChunk(id string, index int, body io.Reader) (blob.UploadStatus, error) {
	status, err := s.Blobs.PutChunk(id, index, body)
	if err != nil {
		return blob.UploadStatus{}, mapUploadError(id, err)
	}
	return status, nil
}

func (s *Service) AbortUpload(id string) error {
	if err := s.Blobs.AbortUpload(id); err != nil {
		return mapUploadError(id, err)
	}
	return nil
}

// FinishUpload is the single promotion gate. All chunk markers must exist,
// the assembled size and whole-content SHA-256 must be proven, and ffprobe
// must accept the media before any Source fact is journaled. A session whose
// chunks merely look complete but whose content is unconfirmed never becomes
// clippable media.
func (s *Service) FinishUpload(ctx context.Context, id, name string) (domain.Source, error) {
	if strings.TrimSpace(name) == "" || len(name) > 160 {
		return domain.Source{}, domain.Invalid("source name must contain 1..160 bytes")
	}
	digest, n, err := s.Blobs.CompleteUpload(id)
	if err != nil {
		return domain.Source{}, mapUploadError(id, err)
	}
	path, _ := s.Blobs.Object(digest)
	metadata, err := s.Probe.Probe(ctx, path)
	if err != nil {
		return domain.Source{}, domain.Invalid("unusable media: %s", err)
	}
	s.mutation.Lock()
	defer s.mutation.Unlock()
	source := domain.Source{ID: ID(), Name: name, Digest: digest, Bytes: n, Media: metadata, CreatedAt: time.Now().UTC()}
	err = s.Store.Update(func(st *domain.State) error {
		for _, existing := range st.Sources {
			if existing.Digest == digest {
				source = existing
				return nil
			}
		}
		st.Sources[source.ID] = source
		st.Emit(source.ID, "source.accepted", name)
		return nil
	})
	return source, err
}
