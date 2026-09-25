package stream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// FormatVersion 为检查点格式版本，不兼容版本必须被拒绝。
const FormatVersion = 1

// Checkpoint 涵盖消费位点、窗口状态、去重集合、水位线与待确认输出。
type Checkpoint struct {
	Version   int
	Seq       uint64
	Offsets   map[int]uint64
	Watermark int64 // unix nano
	Windows   []WindowState
	Dedup     []string
	Pending   []Output // 已产出但未确认的输出
}

// ErrIncompatibleCheckpoint 表示检查点版本不兼容。
var ErrIncompatibleCheckpoint = errors.New("stream: 检查点格式版本不兼容")

// ErrCorruptCheckpoint 表示检查点数据损坏（校验和不匹配或无法解析）。
var ErrCorruptCheckpoint = errors.New("stream: 检查点数据损坏")

// CheckpointStore 持久化检查点。
type CheckpointStore interface {
	Save(ctx context.Context, cp Checkpoint) error
	Load(ctx context.Context) (Checkpoint, bool, error)
}

// envelope 为落盘封装：数据 + SHA-256 校验和。
type envelope struct {
	Data     []byte `json:"data"`
	Checksum string `json:"checksum"`
}

// FileCheckpointStore 以原子写（临时文件 + rename）+ 校验和方式落盘。
type FileCheckpointStore struct {
	Path string
}

func (s *FileCheckpointStore) Save(ctx context.Context, cp Checkpoint) error {
	if err := s.ensureDir(); err != nil {
		return fmt.Errorf("stream: 创建检查点目录失败: %w", err)
	}
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("stream: 序列化检查点失败: %w", err)
	}
	sum := sha256.Sum256(data)
	env, err := json.Marshal(envelope{Data: data, Checksum: hex.EncodeToString(sum[:])})
	if err != nil {
		return fmt.Errorf("stream: 序列化检查点封装失败: %w", err)
	}
	tmp := s.Path + ".tmp"
	if err := os.WriteFile(tmp, env, 0o644); err != nil {
		return fmt.Errorf("stream: 写入临时检查点失败: %w", err)
	}
	if err := os.Rename(tmp, s.Path); err != nil {
		return fmt.Errorf("stream: 提交检查点失败: %w", err)
	}
	return nil
}

func (s *FileCheckpointStore) Load(ctx context.Context) (Checkpoint, bool, error) {
	envBytes, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("stream: 读取检查点失败: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(envBytes, &env); err != nil {
		return Checkpoint{}, false, fmt.Errorf("%w: 封装无法解析", ErrCorruptCheckpoint)
	}
	sum := sha256.Sum256(env.Data)
	if hex.EncodeToString(sum[:]) != env.Checksum {
		return Checkpoint{}, false, fmt.Errorf("%w: 校验和不匹配", ErrCorruptCheckpoint)
	}
	var cp Checkpoint
	if err := json.Unmarshal(env.Data, &cp); err != nil {
		return Checkpoint{}, false, fmt.Errorf("%w: 数据无法解析", ErrCorruptCheckpoint)
	}
	if cp.Version != FormatVersion {
		return Checkpoint{}, false, fmt.Errorf("%w: 期望 %d, 实际 %d", ErrIncompatibleCheckpoint, FormatVersion, cp.Version)
	}
	return cp, true, nil
}

// 确保目录存在（Path 的父目录）。
func (s *FileCheckpointStore) ensureDir() error {
	return os.MkdirAll(filepath.Dir(s.Path), 0o755)
}
