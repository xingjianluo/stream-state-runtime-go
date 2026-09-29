package stream

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestStateSizeStable 长时间大流量下验证状态规模有界：
// 事件时间推进很多天、事件量线性增长，窗口状态/去重集合/确认账本必须稳定在
// 与"水位线附近有界窗口"相当的量级，而不是随已处理事件数增长；
// 检查点文件大小同样保持有界。
func TestStateSizeStable(t *testing.T) {
	const windowSize = 10 * time.Second
	const lateness = 30 * time.Second
	const keys = 8
	const seconds = 30 * 60 // 事件时间推进 30 分钟（远大于容忍窗口）
	const total = seconds * keys

	var events []Event
	for sec := 0; sec < seconds; sec++ {
		for k := 0; k < keys; k++ {
			id := fmt.Sprintf("t%d-k%d", sec, k)
			events = append(events, Event{
				ID: id, Key: fmt.Sprintf("k%d", k), Shard: 0,
				Offset:    uint64(len(events)),
				EventTime: ts(sec),
				Value:     float64(sec + k),
			})
		}
	}
	// 重复投递一批较早的事件（部分所属窗口已过清理线，部分仍在容忍期内）：
	// 容忍期内的必须仍被识别为 duplicate 并保持结果不变。
	var dups []Event
	for sec := seconds - 4; sec < seconds; sec++ {
		for k := 0; k < keys; k++ {
			ev := Event{
				ID: fmt.Sprintf("t%d-k%d", sec, k), Key: fmt.Sprintf("k%d", k), Shard: 0,
				Offset:    uint64(total + len(dups)),
				EventTime: ts(sec),
				Value:     float64(sec + k),
			}
			dups = append(dups, ev)
		}
	}
	events = append(events, dups...)

	dir := t.TempDir()
	store := &FileCheckpointStore{Path: dir + "/cp.json"}
	cfg := Config{
		WindowSize: windowSize, AllowedLateness: lateness,
		FetchBatch: 64, CheckpointEvery: 256, CheckpointStore: store,
	}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	sink := NewMemSink()
	rt := NewRuntime(cfg, &SliceSource{Data: [][]Event{events}}, sink)

	// 运行过程中持续采样状态规模，记录峰值。
	var maxWindows, maxDedup, maxAcks, maxPending int
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				z := rt.StateSize()
				if z.Windows > maxWindows {
					maxWindows = z.Windows
				}
				if z.Dedup > maxDedup {
					maxDedup = z.Dedup
				}
				if z.Acks > maxAcks {
					maxAcks = z.Acks
				}
				if z.Pending > maxPending {
					maxPending = z.Pending
				}
			case <-stop:
				return
			}
		}
	}()
	if err := rt.Run(t.Context()); err != nil {
		close(stop)
		t.Fatal(err)
	}
	close(stop)

	st := rt.Stats()
	size := rt.StateSize()
	t.Logf("总事件=%d (含重复 %d) 处理=%d 修正=%d 清理窗口=%d 检查点=%d",
		len(events), len(dups), st.Processed, st.Corrections, st.Purged, st.Checkpoints)
	t.Logf("结束时状态规模: %+v", size)
	if st.Duplicates != len(dups) {
		t.Fatalf("容忍期内重复件必须全部被识别: duplicates=%d, want %d", st.Duplicates, len(dups))
	}

	// 结束后全部窗口已触发并清理；dedup/acks 同样应被回收。
	if size.Windows != 0 || size.Dedup != 0 || size.Acks != 0 {
		t.Fatalf("最终状态应全部回收: %+v", size)
	}

	// 有界量级推导（均只与水位线附近的时间跨度有关，与总事件量无关）：
	//   窗口状态：触发在 End、清理在 End+Lateness，最多容纳 Lateness/Size+2
	//   个已触发窗口，加上尚未触发窗口；
	//   去重 ID：保留到所属窗口 End+Lateness，最多 (Size+Lateness)/Size+1 个窗口。
	windowBound := (int(lateness/windowSize) + 3) * keys
	// 去重 ID 按事件计：每窗口含 windowSize*keys 条，保留到 End+Lateness。
	dedupPerWindow := int(windowSize/time.Second) * keys
	dedupBound := (int((windowSize+lateness)/windowSize) + 2) * dedupPerWindow
	t.Logf("运行中峰值状态: windows=%d(上界%d) dedup=%d(上界%d) acks=%d pending=%d",
		maxWindows, windowBound, maxDedup, dedupBound, maxAcks, maxPending)
	if maxWindows > windowBound {
		t.Fatalf("窗口状态峰值 %d 超出有界上界 %d（总事件 %d）", maxWindows, windowBound, len(events))
	}
	if maxDedup > dedupBound {
		t.Fatalf("去重集合峰值 %d 超出有界上界 %d（总事件 %d）", maxDedup, dedupBound, len(events))
	}
	// 确认账本只覆盖仍在产出/在途的窗口，量级同样有界。
	if maxAcks > windowBound || maxPending > 2*keys {
		t.Fatalf("确认账本/在途输出峰值异常: acks=%d pending=%d", maxAcks, maxPending)
	}
	if size.Pending != 0 {
		t.Fatalf("结束时不应有未确认输出: %d", size.Pending)
	}
	if n := len(sink.Results()); n != (seconds/10)*keys {
		t.Fatalf("(窗口,键) 结果数: got %d, want %d", n, (seconds/10)*keys)
	}

	// 检查点文件大小必须是有界量级（全部状态已回收，应很小）；
	// 同时对比"不清理时可能达到的规模"：总事件数量级。
	info, err := os.Stat(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	// 即使检查点含最终位点等元数据，也不应超过每 (窗口,键) 一条结果量级的零头。
	hardBound := int64(windowBound * 200) // 每条 JSON 记录远小于 200B，上界极宽松
	if info.Size() > hardBound {
		t.Fatalf("检查点文件未随状态清理收敛: %d 字节, 上界 %d", info.Size(), hardBound)
	}
	t.Logf("检查点文件大小=%d 字节（有界上界 %d），总事件量=%d",
		info.Size(), hardBound, len(events))
}
