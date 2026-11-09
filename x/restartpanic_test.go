package x

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 测试里把重启间隔压到毫秒级。
// 注意：AutoRestartPanic 没有停止句柄，所以每个用例的 startAndBlock
// 都必须在有限次调用后返回非 nil error，让内部 goroutine 走 break 退出，
// 避免给测试进程留下永久运行的 goroutine。
const (
	testDuration = 10 * time.Millisecond
	quietWindow  = 150 * time.Millisecond // 远大于 testDuration，用于确认"不再重启"
	waitTimeout  = 3 * time.Second
)

type testCtxKey struct{}

// ================= Option =================

func TestOptionsApply(t *testing.T) {
	o := &option{}
	wantCtx := context.WithValue(context.Background(), testCtxKey{}, "v")
	wantDuration := 7 * time.Second

	for _, f := range []Option{
		WithName("worker-1"),
		WithDuration(wantDuration),
		WithCtx(wantCtx),
	} {
		f(o)
	}

	if o.name != "worker-1" {
		t.Errorf("name = %q, want %q", o.name, "worker-1")
	}
	if o.duration != wantDuration {
		t.Errorf("duration = %s, want %s", o.duration, wantDuration)
	}
	if o.ctx != wantCtx {
		t.Error("ctx 未按原样保存")
	}
	if got := o.ctx.Value(testCtxKey{}); got != "v" {
		t.Errorf("ctx.Value = %v, want %q", got, "v")
	}
}

// options 是函数切片，按传入顺序依次执行，后一个覆盖前一个。
// 若将来改成 map 或并发 apply，本用例会失败。
func TestOptionsOverrideInOrder(t *testing.T) {
	o := &option{}
	first := context.WithValue(context.Background(), testCtxKey{}, "first")
	second := context.WithValue(context.Background(), testCtxKey{}, "second")

	opts := []Option{
		WithName("a"), WithName("b"),
		WithDuration(time.Second), WithDuration(2 * time.Second),
		WithCtx(first), WithCtx(second),
	}
	for _, f := range opts {
		f(o)
	}

	if o.name != "b" {
		t.Errorf("name = %q, want b", o.name)
	}
	if o.duration != 2*time.Second {
		t.Errorf("duration = %s, want 2s", o.duration)
	}
	if got := o.ctx.Value(testCtxKey{}); got != "second" {
		t.Errorf("ctx.Value = %v, want second", got)
	}
}

// WithName / WithCtx / WithDuration 返回的都是 Option（func(*option)），
// 可以作为值传递、延迟应用。
func TestOptionsAreValues(t *testing.T) {
	f := WithDuration(3 * time.Second)
	o := &option{}
	f(o)

	if o.duration != 3*time.Second {
		t.Fatalf("duration = %s, want 3s", o.duration)
	}
}

// ================= AutoRestartPanic =================

// 轮询等待调用次数达到 want。
func waitCalls(t *testing.T, p *int32, want int32) {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(p) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待第 %d 次调用超时，实际 %d 次", want, atomic.LoadInt32(p))
}

// 关键前提：go-log 的 Fatal 只打印 FATAL 日志 + panic trace，
// 并不调用 os.Exit（见 log/logger.go:120 / log/log.go:148）。
// 所以 recover 之后 goroutine 能继续执行到"重启"分支，测试进程也不会被杀掉。
//
// panic 时 "err = startAndBlock(ctx)" 的赋值不会发生，err 保持上一轮的值；
// 而上一轮若 err != nil 循环已经 break，因此 panic 时 err 必然是 nil → 重启。
func TestAutoRestartPanicRestartsAfterPanic(t *testing.T) {
	var calls int32

	AutoRestartPanic(func(ctx context.Context) error {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			panic("boom")
		}
		return errors.New("stop")
	}, WithName("test-panic"), WithDuration(testDuration))

	waitCalls(t, &calls, 2)
	time.Sleep(quietWindow)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("调用次数 = %d, want 2（panic 后重启一次，返回 error 后停止）", got)
	}
}

// 连续两轮 panic 都应重启：钉住 err 跨轮复用时始终为 nil 的行为
func TestAutoRestartPanicRestartsAfterRepeatedPanics(t *testing.T) {
	var calls int32

	AutoRestartPanic(func(ctx context.Context) error {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			panic("boom")
		}
		return errors.New("stop")
	}, WithName("test-panic-twice"), WithDuration(testDuration))

	waitCalls(t, &calls, 3)
	time.Sleep(quietWindow)
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("调用次数 = %d, want 3", got)
	}
}

// startAndBlock 返回 nil 也视为"异常结束"，同样走重启分支
func TestAutoRestartPanicRestartsOnNilError(t *testing.T) {
	var calls int32

	AutoRestartPanic(func(ctx context.Context) error {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return nil
		}
		return errors.New("stop")
	}, WithName("test-nil"), WithDuration(testDuration))

	waitCalls(t, &calls, 2)
	time.Sleep(quietWindow)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("调用次数 = %d, want 2", got)
	}
}

// 返回非 nil error 是唯一的正常停止路径：不能再重启
func TestAutoRestartPanicStopsOnError(t *testing.T) {
	var calls int32

	AutoRestartPanic(func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("bye")
	}, WithName("test-stop"), WithDuration(testDuration))

	waitCalls(t, &calls, 1)
	time.Sleep(quietWindow)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("调用次数 = %d, want 1（返回 error 后不应重启）", got)
	}
}

// WithCtx 传入的 ctx 必须能透传到 startAndBlock（log.WithCtx 只包一层 value，不丢上游 value）
func TestAutoRestartPanicPassesCtxValue(t *testing.T) {
	parent := context.WithValue(context.Background(), testCtxKey{}, "from-parent")
	got := make(chan interface{}, 1)

	AutoRestartPanic(func(ctx context.Context) error {
		got <- ctx.Value(testCtxKey{})
		return errors.New("stop")
	}, WithName("test-ctx"), WithCtx(parent), WithDuration(testDuration))

	select {
	case v := <-got:
		if v != "from-parent" {
			t.Fatalf("ctx.Value = %v, want %q", v, "from-parent")
		}
	case <-time.After(waitTimeout):
		t.Fatal("startAndBlock 未被调用")
	}
}

// 已取消的父 ctx：派生的 ctx 也应是 canceled。
// 当前实现不因 ctx 取消而跳过调用，startAndBlock 仍会被调用一次。
func TestAutoRestartPanicCanceledCtx(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	got := make(chan error, 1)
	AutoRestartPanic(func(ctx context.Context) error {
		got <- ctx.Err()
		return errors.New("stop")
	}, WithName("test-canceled"), WithCtx(parent), WithDuration(testDuration))

	select {
	case err := <-got:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx.Err() = %v, want context.Canceled", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("startAndBlock 未被调用")
	}
}

// 每轮用 context.WithCancel(ctx) 派生新 ctx，并在轮次结束时 defer cancel()；
// 所以进入第二轮时，第一轮的 ctx 应已被取消。
// 这保证上一轮的 startAndBlock（若遗留了子 goroutine）不会继续跑。
func TestAutoRestartPanicCancelsRoundCtx(t *testing.T) {
	var firstCtx context.Context
	var round int
	result := make(chan error, 1)

	AutoRestartPanic(func(ctx context.Context) error {
		round++
		if round == 1 {
			firstCtx = ctx
			return nil // 触发重启
		}
		result <- firstCtx.Err()
		return errors.New("stop")
	}, WithName("test-round-ctx"), WithDuration(testDuration))

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("第一轮 ctx.Err() = %v, want context.Canceled", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("未进入第二轮（返回 nil 后应重启）")
	}
}

// 说明：
//   - AutoRestartPanic 不返回句柄，也没法注入时钟，所以"是否重启"只能靠
//     调用计数 + 短窗口观察。testDuration 取 10ms、quietWindow 取 150ms，
//     足以覆盖一次潜在重启而不让用例变慢。
//   - 默认值（name = exe.Name、duration = 30s、ctx = context.Background()）
//     无法在不修改实现的前提下直接观察：真的用默认值会 sleep 30s。
//     这三个默认值由 Option 的覆盖用例间接保证（WithXxx 能改掉对应字段）。
