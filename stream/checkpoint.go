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
//
// 版本历史：
//   - v1：窗口触发后即删除状态；去重集合为纯 ID 列表；无修正版本/确认账本。
//   - v2：窗口触发后保留到迟到容忍期结束并支持版本化修正（WindowState 新增
//     Fired/Revision，Output 新增 Revision）；去重集合携带事件时间用于安全 GC；
//     新增每个输出 ID 的已确认最高版本 Acks。
const FormatVersion = 2

// MinSupportedVersion 为可接受的最低检查点版本。
const MinSupportedVersion = FormatVersion

// DedupEntry 是检查点中的去重条目：事件 ID 与其所属窗口的清理时刻（UnixNano）。
type DedupEntry struct {
	ID      string `json:"id"`
	PurgeAt int64  `json:"purge_at"`
}

// Checkpoint 涵盖消费位点、窗口状态、去重集合、水位线、待确认输出与确认账本。
type Checkpoint struct {
	Version   int    `json:"version"`
	Seq       uint64 `json:"seq"`
	Offsets   map[int]uint64
	Watermark int64 // unix nano
	Windows   []WindowState
	Dedup     []DedupEntry
	Pending   []Output       // 已产出但未确认的输出（含修正版本）
	Acks      map[string]int // 每个输出 ID 已确认的最高版本（修正进度）
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
		switch {
		case cp.Version < MinSupportedVersion:
			return Checkpoint{}, false, fmt.Errorf(
				"%w: 检查点来自旧版本 %d，低于最低支持版本 %d（当前 %d），格式不兼容；需用旧版本处理或丢弃该检查点",
				ErrIncompatibleCheckpoint, cp.Version, MinSupportedVersion, FormatVersion)
		default:
			return Checkpoint{}, false, fmt.Errorf(
				"%w: 检查点来自更新版本 %d，本运行时仅支持 %d，拒绝加载以避免破坏状态",
				ErrIncompatibleCheckpoint, cp.Version, FormatVersion)
		}
	}
	return cp, true, nil
}

// 确保目录存在（Path 的父目录）。
func (s *FileCheckpointStore) ensureDir() error {
	return os.MkdirAll(filepath.Dir(s.Path), 0o755)
}
