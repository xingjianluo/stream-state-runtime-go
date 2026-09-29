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

// FormatVersion 为当前检查点格式版本。
//
// 版本历史：
//   - v1：窗口触发即清理、去重集合仅存 ID 列表；
//   - v2：窗口首次触发后保留以支持迟到修正（WindowState 含 Revision/Fired）、
//     去重集合携带事件时间以支持安全回收（DedupEntry）、pending 携带 Revision。
const FormatVersion = 2

// MinSupportedVersion 为可接受的最低检查点版本，低于此版本明确拒绝。
const MinSupportedVersion = 2

// Checkpoint 涵盖消费位点、窗口状态、去重集合、水位线与待确认输出。
type Checkpoint struct {
	Version   int
	Seq       uint64
	Offsets   map[int]uint64
	Watermark int64 // unix nano
	Windows   []WindowState
	Dedup     []DedupEntry
	Pending   []Output // 已产出但未确认的输出（含首次触发与修正）
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
	// 版本兼容范围：恰好等于当前格式版本才接受；
	// 旧版本（含缺失字段的 v1）与更新版本都明确拒绝，绝不静默按当前格式继续。
	switch {
	case cp.Version < MinSupportedVersion:
		return Checkpoint{}, false, fmt.Errorf(
			"%w: 检查点来自不兼容的旧版本 %d（最低支持 %d，当前 %d），需按迁移流程处理后才能启动",
			ErrIncompatibleCheckpoint, cp.Version, MinSupportedVersion, FormatVersion)
	case cp.Version > FormatVersion:
		return Checkpoint{}, false, fmt.Errorf(
			"%w: 检查点来自更新的版本 %d（当前二进制仅支持 %d），请升级后再启动",
			ErrIncompatibleCheckpoint, cp.Version, FormatVersion)
	}
	return cp, true, nil
}

// 确保目录存在（Path 的父目录）。
func (s *FileCheckpointStore) ensureDir() error {
	return os.MkdirAll(filepath.Dir(s.Path), 0o755)
}
