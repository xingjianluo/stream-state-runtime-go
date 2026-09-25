package streamruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// checkpointVersion 是检查点格式版本，不兼容时必须拒绝恢复。
const checkpointVersion = 1

// checkpointMagic 是检查点文件的起始魔数。
const checkpointMagic = "SRCP"

// windowSnapshot 是单个窗口的可序列化快照。
type windowSnapshot struct {
	Start      time.Time            `json:"start"`
	End        time.Time            `json:"end"`
	KeyAggs    map[string]Aggregate `json:"key_aggs"`
	KeyFlushed map[string]bool      `json:"key_flushed"`
	EventIDs   map[string]struct{}  `json:"event_ids"`
}

// pendingOutputSnapshot 是一条待下游确认的输出快照。
type pendingOutputSnapshot struct {
	Record OutputRecord `json:"record"`
}

// Checkpoint 是运行时某一时刻的完整一致状态快照。
type Checkpoint struct {
	Version        int                     `json:"version"`
	ID             string                  `json:"id"`
	CreatedAt      time.Time               `json:"created_at"`
	EventWatermark time.Time               `json:"event_watermark"`
	Offsets        map[int]int64           `json:"offsets"`
	Windows        []windowSnapshot        `json:"windows"`
	NextOutputSeq  int64                   `json:"next_output_seq"`
	PendingOutputs []pendingOutputSnapshot `json:"pending_outputs"`
}

// validate 对反序列化后的检查点做严格语义校验，任何不一致都视为损坏。
func (cp *Checkpoint) validate() error {
	if cp.Version != checkpointVersion {
		return fmt.Errorf("%w: version=%d want=%d", ErrIncompatibleCheckpoint, cp.Version, checkpointVersion)
	}
	for shard, off := range cp.Offsets {
		if shard < 0 {
			return fmt.Errorf("%w: negative shard id %d", ErrCorruptCheckpoint, shard)
		}
		if off < 0 {
			return fmt.Errorf("%w: negative offset %d for shard %d", ErrCorruptCheckpoint, off, shard)
		}
	}
	for i, w := range cp.Windows {
		if !w.End.After(w.Start) {
			return fmt.Errorf("%w: window %d end(%s) <= start(%s)", ErrCorruptCheckpoint, i, w.End.Format(time.RFC3339Nano), w.Start.Format(time.RFC3339Nano))
		}
		if w.KeyAggs == nil || w.EventIDs == nil || w.KeyFlushed == nil {
			return fmt.Errorf("%w: window %d missing maps", ErrCorruptCheckpoint, i)
		}
		for k, agg := range w.KeyAggs {
			if agg.Count < 0 || agg.Count == 0 {
				return fmt.Errorf("%w: window %d key %q invalid count %d", ErrCorruptCheckpoint, i, k, agg.Count)
			}
		}
		for k := range w.KeyFlushed {
			if _, ok := w.KeyAggs[k]; !ok {
				return fmt.Errorf("%w: window %d flushed key %q missing aggregate", ErrCorruptCheckpoint, i, k)
			}
		}
	}
	// Pending 输出的 Seq 必须严格递增，且全部小于 NextOutputSeq。
	var prev int64
	for i, p := range cp.PendingOutputs {
		if p.Record.Seq <= 0 {
			return fmt.Errorf("%w: pending %d has non-positive seq", ErrCorruptCheckpoint, i)
		}
		if i > 0 && p.Record.Seq <= prev {
			return fmt.Errorf("%w: pending seq not strictly increasing at %d", ErrCorruptCheckpoint, i)
		}
		if p.Record.Seq >= cp.NextOutputSeq {
			return fmt.Errorf("%w: pending seq %d >= next_seq %d", ErrCorruptCheckpoint, p.Record.Seq, cp.NextOutputSeq)
		}
		prev = p.Record.Seq
	}
	return nil
}

// marshalPayload 输出检查点的规范 JSON 载荷。
func (cp *Checkpoint) marshalPayload() ([]byte, error) {
	if cp.Version == 0 {
		cp.Version = checkpointVersion
	}
	if cp.ID == "" {
		cp.ID = newCheckpointID()
	}
	payload, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: marshal: %v", ErrCorruptCheckpoint, err)
	}
	return payload, nil
}

// frame 格式：
//
//	SRCP\n
//	<version>\n
//	<sha256-hex(json)>\n
//	<json bytes>
func writeSnapshot(w io.Writer, cp *Checkpoint) error {
	payload, err := cp.marshalPayload()
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	header := fmt.Sprintf("%s\n%d\n%s\n", checkpointMagic, checkpointVersion, hex.EncodeToString(sum[:]))
	if _, err := io.Copy(w, bytes.NewReader(append([]byte(header), payload...))); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

// readSnapshot 从 r 读取检查点，并严格校验魔数、版本与校验和。
func readSnapshot(r io.Reader) (*Checkpoint, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	nl := func(from int) int { return bytes.IndexByte(raw[from:], '\n') }
	first := nl(0)
	if first < 0 || string(raw[:first]) != checkpointMagic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorruptCheckpoint)
	}
	pos := first + 1
	second := nl(pos)
	if second < 0 {
		return nil, fmt.Errorf("%w: truncated header", ErrCorruptCheckpoint)
	}
	second += pos
	var version int
	if _, err := fmt.Sscanf(string(raw[pos:second]), "%d", &version); err != nil || version != checkpointVersion {
		return nil, fmt.Errorf("%w: unsupported frame version %q", ErrIncompatibleCheckpoint, strings.TrimSpace(string(raw[pos:second])))
	}
	pos = second + 1
	third := nl(pos)
	if third < 0 {
		return nil, fmt.Errorf("%w: truncated checksum", ErrCorruptCheckpoint)
	}
	third += pos
	wantSum, err := hex.DecodeString(strings.TrimSpace(string(raw[pos:third])))
	if err != nil || len(wantSum) != sha256.Size {
		return nil, fmt.Errorf("%w: malformed checksum", ErrCorruptCheckpoint)
	}
	payload := raw[third+1:]
	gotSum := sha256.Sum256(payload)
	if !bytes.Equal(wantSum, gotSum[:]) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptCheckpoint)
	}

	var cp Checkpoint
	if err := json.Unmarshal(payload, &cp); err != nil {
		return nil, fmt.Errorf("%w: invalid json: %v", ErrCorruptCheckpoint, err)
	}
	if err := cp.validate(); err != nil {
		return nil, err
	}
	return &cp, nil
}
