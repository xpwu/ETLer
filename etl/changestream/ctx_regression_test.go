package changestream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 护栏：syncChan 保持无缓冲。
//
// 注意：这不再是「握手正确性的前提」。sendSyncAck 用 select 带 ctx.Done()，
// 无缓冲不再意味着可能永久阻塞；正确性由第二个 select 等 syncAck 回执保证。
// 保留此用例只是提醒：若把 syncChan 改成缓冲 channel，需要复核「过期的
// ack channel 滞留在 channel 里、被下一轮 scheduler 取走做多余 force sync」。
// ---------------------------------------------------------------------------
func TestSyncAckChannelIsUnbuffered(t *testing.T) {
	if cap(syncChan) != 0 {
		t.Fatalf("syncChan cap = %d, want 0（改为缓冲前请复核 sendSyncAck 的 ack 滞留语义）", cap(syncChan))
	}
}

// ---------------------------------------------------------------------------
// 路径 1：ctx 在【第一阶段 send 之前】就已取消。
// syncChan 无缓冲且无接收方 → send case 不 ready → 必走 <-ctx.Done()，
// 返回 ErrShuttingDown，且不阻塞。
// ---------------------------------------------------------------------------
func TestSyncAckHandshakeRespectsCanceledCtx(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 用 chan *StreamError 而不是 chan error：*StreamError 实现 error 接口，
	// 若 sendSyncAck 返回 nil 指针，装进 error 接口会变成 typed-nil（err != nil
	// 成立），后续断言会误判。精确类型没有这个隐患。
	done := make(chan *StreamError, 1)
	go func() { done <- sendSyncAck(ctx, syncChan) }()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("err = %v, want ErrShuttingDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 已取消但握手仍阻塞：send/recv 未响应 ctx")
	}
}

// ---------------------------------------------------------------------------
// 路径 2：正常往返 —— scheduler 取走 ack channel 并回 ack，握手应返回 nil。
// 复刻 scheduler.go 的 case ack := <-changestream.NeedForceSync() 分支。
// ---------------------------------------------------------------------------
func TestSyncAckHandshakeCompletesWhenSchedulerAcks(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	ctx := context.Background()
	done := make(chan *StreamError, 1)
	go func() { done <- sendSyncAck(ctx, syncChan) }()

	var ack SyncAckChan
	select {
	case ack = <-syncChan:
	case <-time.After(2 * time.Second):
		t.Fatal("sendSyncAck 没有把 ack channel 交给 syncChan")
	}
	ack <- struct{}{}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("正常握手 err = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("收到 ack 后握手仍未返回")
	}
}

// ---------------------------------------------------------------------------
// 路径 3：ctx 在【第二阶段等待 ack】期间取消 —— 故意不回 ack，只 cancel。
// 这是旧实现真正的死锁点：裸 `<-syncAck` 不检查 ctx。
// ---------------------------------------------------------------------------
func TestSyncAckHandshakeRespectsCancelWhileWaitingAck(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan *StreamError, 1)
	go func() { done <- sendSyncAck(ctx, syncChan) }()

	// 取走 ack channel，让 sendSyncAck 进入第二个 select
	select {
	case <-syncChan:
	case <-time.After(2 * time.Second):
		t.Fatal("sendSyncAck 没有把 ack channel 交给 syncChan")
	}

	cancel()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("err = %v, want ErrShuttingDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待 ack 期间 ctx 取消，握手仍阻塞")
	}
}
