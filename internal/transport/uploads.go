package transport

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/service"
	"net/http"
	"strconv"
)

func (a API) createUpload(w http.ResponseWriter, r *http.Request) {
	var input service.CreateUploadRequest
	if err := decode(w, r, &input); err != nil {
		failure(w, err)
		return
	}
	status, err := a.Service.CreateUpload(input)
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 201, status)
}

func (a API) uploadStatus(w http.ResponseWriter, r *http.Request) {
	status, err := a.Service.UploadStatus(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 200, status)
}

func (a API) putChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	index, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || index < 0 {
		failure(w, domain.Invalid("chunk index must be a non-negative integer"))
		return
	}
	// Bound the body to one chunk plus one byte so an oversized upload fails
	// deterministically instead of being silently truncated. The exact size is
	// revalidated against the session layout in the blob layer.
	r.Body = http.MaxBytesReader(w, r.Body, blob.MaxChunkBytes+1)
	status, err := a.Service.PutChunk(id, index, r.Body)
	if err != nil {
		failure(w, err)
		return
	}
	w.Header().Set("ETag", strconv.Itoa(len(status.Received)))
	write(w, 200, status)
}

func (a API) completeUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	name := r.URL.Query().Get("name")
	source, err := a.Service.FinishUpload(r.Context(), id, name)
	if err != nil {
		failure(w, err)
		return
	}
	write(w, 201, source)
}

func (a API) abortUpload(w http.ResponseWriter, r *http.Request) {
	if err := a.Service.AbortUpload(r.PathValue("id")); err != nil {
		failure(w, err)
		return
	}
	w.WriteHeader(204)
}
