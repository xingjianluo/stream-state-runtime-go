package streamruntime

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	lumierand "crypto/rand"
	"encoding/hex"
)

// CheckpointStore 抽象检查点持久化介质。
type CheckpointStore interface {
	Save(cp *Checkpoint) error
	LoadLatest() (*Checkpoint, error)
}

// FileStore 把检查点原子地写入单个本地文件（临时文件 + fsync + rename）。
type FileStore struct {
	path string
}

// NewFileStore 创建基于本地文件的检查点存储。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// newCheckpointID 生成一个短随机检查点 ID，仅用于审计辨识。
func newCheckpointID() string {
	var b [8]byte
	_, _ = lumierand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Save 原子持久化检查点：写临时文件、fsync、rename 替换，避免崩溃产生半写文件。
func (s *FileStore) Save(cp *Checkpoint) error {
	if cp.ID == "" {
		cp.ID = newCheckpointID()
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".cp-*")
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	var buf bytes.Buffer
	if err := writeSnapshot(&buf, cp); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	// 尽量持久化目录项；失败不影响本次文件内容的完整性。
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// LoadLatest 读取并严格校验最新检查点；文件不存在时返回 ErrNoCheckpoint。
func (s *FileStore) LoadLatest() (*Checkpoint, error) {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNoCheckpoint
		}
		return nil, fmt.Errorf("open checkpoint: %w", err)
	}
	defer f.Close()
	return readSnapshot(f)
}

var _ CheckpointStore = (*FileStore)(nil)
