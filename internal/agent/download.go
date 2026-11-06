package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/donor"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/storage"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

type partial struct {
	Offset    uint64 `json:"offset"`
	Checksum  string `json:"checksum"`
	ETag      string `json:"etag"`
	Validator string `json:"validator"`
	Total     uint64 `json:"total"`
}

func ParseContentRange(s string) (uint64, uint64, uint64, error) {
	var from, to, total uint64
	var tail string
	n, e := fmt.Sscanf(s, "bytes %d-%d/%d%s", &from, &to, &total, &tail)
	if (e != nil && e != io.EOF) || n != 3 || from > to || to >= total {
		return 0, 0, 0, errors.New("invalid_content_range")
	}
	return from, to, total, nil
}
func (a *Runner) download(x *execution, result model.ItemResult, raw string, cfg model.JobConfig) (uint64, string, error) {
	root, e := a.Volume.OpenRoot()
	if e != nil {
		return 0, "", e
	}
	defer root.Close()
	stage := storage.Stage(a.Config.WorkerID, x.lease.AttemptID, result.StageID)
	if e = storage.MkdirAll(root, stage, 0770); e != nil {
		return 0, "", e
	}
	metadata := *result.Metadata
	metadata.DownloadURL = ""
	if e = storage.WriteJSON(root, path.Join(stage, "metadata.json"), metadata); e != nil {
		return 0, "", e
	}
	filePath := path.Join(stage, "video.part")
	manifest := path.Join(stage, "partial.json")
	state := partial{Total: metadata.Size}
	reserved := result.ReserveBytes / 2
	if f, err := root.Open(manifest); err == nil {
		err = json.NewDecoder(io.LimitReader(f, 4096)).Decode(&state)
		f.Close()
		if err != nil {
			state = partial{Total: metadata.Size}
		}
	}
	reset := func() error {
		state = partial{Total: metadata.Size}
		f, e := root.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
		if e != nil {
			return e
		}
		return f.Close()
	}
	checkpoint := func() error {
		size, hash, e := storage.Hash(root, filePath)
		if e != nil {
			return e
		}
		state.Offset = size
		state.Checksum = hash
		return storage.WriteJSON(root, manifest, state)
	}
	for attempt := 0; attempt <= cfg.Retry.MaxRetries; attempt++ {
		if x.ctx.Err() != nil {
			return 0, "", x.ctx.Err()
		}
		if _, e = a.Volume.Check(); e != nil {
			return 0, "", e
		}
		if state.Offset > 0 {
			size, hash, err := storage.Hash(root, filePath)
			if err != nil || size != state.Offset || hash != state.Checksum || state.Validator == "" {
				if e = reset(); e != nil {
					return 0, "", e
				}
			}
		}
		if state.Total > 0 && state.Offset == state.Total {
			size, checksum, e := storage.Hash(root, filePath)
			return size, checksum, e
		}
		headers := http.Header{}
		if state.Offset > 0 {
			headers.Set("Range", fmt.Sprintf("bytes=%d-", state.Offset))
			headers.Set("If-Range", state.Validator)
		}
		response, err := a.HTTP.Get(x.ctx, x.lease.Ref(), "download", raw, headers, time.Duration(cfg.HTTPTimeoutMS)*time.Millisecond)
		if err != nil {
			e = err
			if attempt < cfg.Retry.MaxRetries && sleep(x.ctx, donor.RetryDelay(cfg.Retry, attempt, 0)) {
				continue
			}
			break
		}
		if response.StatusCode == 401 || response.StatusCode == 403 {
			response.Body.Close()
			return 0, "", errors.New("download_forbidden")
		}
		if response.StatusCode == 416 {
			response.Body.Close()
			if state.Total > 0 && state.Offset == state.Total {
				size, checksum, e := storage.Hash(root, filePath)
				return size, checksum, e
			}
			if e = reset(); e != nil {
				return 0, "", e
			}
			continue
		}
		if response.StatusCode == 429 || response.StatusCode >= 500 {
			delay := max(donor.RetryAfter(response.Header.Get("Retry-After"), time.Now()), donor.RetryDelay(cfg.Retry, attempt, 0))
			response.Body.Close()
			if !sleep(x.ctx, delay) {
				return 0, "", x.ctx.Err()
			}
			e = errors.New("download_retry_exhausted")
			continue
		}
		if response.StatusCode != 200 && response.StatusCode != 206 {
			response.Body.Close()
			return 0, "", errors.New("unexpected_download_status")
		}
		if response.StatusCode == 206 {
			from, _, total, err := ParseContentRange(response.Header.Get("Content-Range"))
			if err != nil || from != state.Offset || total > cfg.MaxFileBytes || state.Total > 0 && total != state.Total || state.ETag != "" && response.Header.Get("ETag") != state.ETag {
				response.Body.Close()
				if e = reset(); e != nil {
					return 0, "", e
				}
				continue
			}
			state.Total = total
		} else {
			if state.Offset > 0 {
				if e = reset(); e != nil {
					response.Body.Close()
					return 0, "", e
				}
			}
			if response.ContentLength > 0 {
				if uint64(response.ContentLength) > cfg.MaxFileBytes {
					response.Body.Close()
					return 0, "", errors.New("file_size_limit")
				}
				state.Total = uint64(response.ContentLength)
			}
		}
		state.ETag = response.Header.Get("ETag")
		state.Validator = state.ETag
		if state.Validator == "" || strings.HasPrefix(state.Validator, "W/") {
			state.Validator = response.Header.Get("Last-Modified")
		}
		if ct := response.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "video/") && !strings.HasPrefix(ct, "application/octet-stream") {
			response.Body.Close()
			return 0, "", errors.New("invalid_video_mime")
		}
		if state.Total > reserved {
			result.Status = "downloading"
			result.Metadata = nil
			result.Offset = state.Offset
			result.ETag = state.ETag
			result.ReserveBytes = state.Total * 2
			if _, e = a.report(x, result); e != nil {
				response.Body.Close()
				return 0, "", e
			}
			reserved = state.Total
		}
		flags := os.O_CREATE | os.O_WRONLY
		if state.Offset == 0 {
			flags |= os.O_TRUNC
		}
		f, err := root.OpenFile(filePath, flags, 0640)
		if err != nil {
			response.Body.Close()
			return 0, "", err
		}
		if _, err = f.Seek(int64(state.Offset), io.SeekStart); err != nil {
			f.Close()
			response.Body.Close()
			return 0, "", err
		}
		buffer := make([]byte, 64<<10)
		started := time.Now()
		initial := state.Offset
		lastCheck := state.Offset
		readErr := error(nil)
		for {
			n, err := response.Body.Read(buffer)
			if n > 0 {
				if uint64(n) > cfg.MaxFileBytes-state.Offset {
					readErr = errors.New("file_size_limit")
					break
				}
				if state.Offset+uint64(n) > reserved {
					next := min(cfg.MaxFileBytes, reserved+(8<<20))
					result.Status = "downloading"
					result.Metadata = nil
					result.Offset = state.Offset
					result.ETag = state.ETag
					result.ReserveBytes = next * 2
					if _, e = a.report(x, result); e != nil {
						readErr = e
						break
					}
					reserved = next
				}
				if state.Offset-lastCheck >= 1<<20 || state.Offset == initial {
					if _, e = a.Volume.Check(); e != nil {
						readErr = e
						break
					}
					lastCheck = state.Offset
				}
				written, we := f.Write(buffer[:n])
				state.Offset += uint64(written)
				a.downloadBytes.Add(uint64(written))
				a.mu.Lock()
				d := a.downloads[result.DownloadID]
				d.Offset = state.Offset
				d.Size = state.Total
				d.BytesPerSecond = float64(state.Offset-initial) / max(.001, time.Since(started).Seconds())
				a.downloads[result.DownloadID] = d
				a.mu.Unlock()
				if we != nil || written != n {
					readErr = errors.New("storage_write_failed")
					break
				}
			}
			if err != nil {
				if err != io.EOF {
					readErr = err
				}
				break
			}
			if x.ctx.Err() != nil {
				readErr = x.ctx.Err()
				break
			}
		}
		if syncErr := f.Sync(); syncErr != nil {
			readErr = syncErr
		}
		f.Close()
		response.Body.Close()
		if err := checkpoint(); err != nil {
			return 0, "", err
		}
		if readErr == nil && (state.Total == 0 || state.Offset == state.Total) && state.Offset > 0 {
			if e = storage.SyncDir(root, stage); e != nil {
				return 0, "", e
			}
			return state.Offset, state.Checksum, nil
		}
		e = readErr
		if e == nil {
			e = errors.New("incomplete_download")
		}
		if attempt < cfg.Retry.MaxRetries && !sleep(x.ctx, donor.RetryDelay(cfg.Retry, attempt, 0)) {
			return 0, "", x.ctx.Err()
		}
	}
	if e == nil {
		e = errors.New("download_retry_exhausted")
	}
	return 0, "", e
}
