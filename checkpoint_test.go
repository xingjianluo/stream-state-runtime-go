package streamruntime

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleCheckpoint() *Checkpoint {
	return &Checkpoint{
		Version:        checkpointVersion,
		CreatedAt:      sec(1000),
		EventWatermark: sec(60),
		Offsets:        map[int]int64{0: 5, 1: 7},
		Windows: []windowSnapshot{{
			Start:      sec(0),
			End:        sec(60),
			KeyAggs:    map[string]Aggregate{"k": {Sum: 15, Count: 2}},
			KeyFlushed: map[string]bool{"k": true},
			EventIDs:   map[string]struct{}{"a": {}, "b": {}},
		}},
		NextOutputSeq: 2,
		PendingOutputs: []pendingOutputSnapshot{
			{Record: OutputRecord{Seq: 1, Key: "k", WindowStart: sec(0), WindowEnd: sec(60), Sum: 15, Count: 2}},
		},
	}
}

func TestCheckpointRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	cp := sampleCheckpoint()
	if err := writeSnapshot(&buf, cp); err != nil {
		t.Fatal(err)
	}
	got, err := readSnapshot(&buf)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got.NextOutputSeq != 2 || got.Offsets[1] != 7 || !got.EventWatermark.Equal(sec(60)) {
		t.Fatalf("content mismatch: %+v", got)
	}
	if len(got.Windows) != 1 || got.Windows[0].KeyAggs["k"].Sum != 15 || !got.Windows[0].KeyFlushed["k"] {
		t.Fatalf("window mismatch: %+v", got.Windows)
	}
	t.Logf("检查点往返成功 id=%s wm=%s offsets=%v pending=%d nextSeq=%d",
		got.ID, got.EventWatermark.Format("15:04:05"), got.Offsets, len(got.PendingOutputs), got.NextOutputSeq)
}

func TestCheckpointCorruptRejected(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, sampleCheckpoint()); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	// 翻转载荷区最后一个字节，校验和必须不匹配。
	raw[len(raw)-1] ^= 0xFF
	if _, err := readSnapshot(bytes.NewReader(raw)); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("expect ErrCorruptCheckpoint, got %v", err)
	}
	t.Logf("损坏检查点被拒绝原因=%v", errCorrupt(raw))

	// 魔数错误。
	badMagic := append([]byte("XXXX\n"), raw[5:]...)
	if _, err := readSnapshot(bytes.NewReader(badMagic)); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("bad magic: expect corrupt, got %v", err)
	}
}

func errCorrupt(raw []byte) error {
	_, err := readSnapshot(bytes.NewReader(raw))
	return err
}

func TestCheckpointIncompatibleVersion(t *testing.T) {
	var buf bytes.Buffer
	cp := sampleCheckpoint()
	if err := writeSnapshot(&buf, cp); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	// 帧头版本号改写为 99（载荷版本仍是 1，帧解析先拒绝）。
	raw2 := bytes.Replace(raw, []byte("SRCP\n1\n"), []byte("SRCP\n99\n"), 1)
	_, err := readSnapshot(bytes.NewReader(raw2))
	if !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("frame version: expect incompatible, got %v", err)
	}
	t.Logf("不兼容帧版本被拒绝: %v", err)
}

func TestFileStoreRoundtripAndNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.bin")
	store := NewFileStore(path)

	if _, err := store.LoadLatest(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("expect ErrNoCheckpoint, got %v", err)
	}
	if err := store.Save(sampleCheckpoint()); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadLatest()
	if err != nil {
		t.Fatal(err)
	}
	if got.Offsets[0] != 5 {
		t.Fatalf("offset mismatch: %+v", got)
	}
	// 再次保存覆盖，不产生残留临时文件。
	if err := store.Save(sampleCheckpoint()); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), "cp.bin") {
		t.Fatalf("unexpected dir contents: %v", entries)
	}
}
