package changestream

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

func assertCode(t *testing.T, got *StreamError, want *StreamError, name string) {
	t.Helper()

	if got == nil {
		t.Fatalf("%s: got nil, want code=%d (%s)", name, want.Code, want.Reason)
	}
	if got.Code != want.Code {
		t.Errorf("%s: Code = %d, want %d", name, got.Code, want.Code)
	}
	if !errors.Is(got, want) {
		t.Errorf("%s: errors.Is(got, %s) = false", name, want.Reason)
	}
}

func TestStreamErrorError(t *testing.T) {
	cases := []struct {
		name string
		err  *StreamError
		want string
	}{
		{"只有 Reason", &StreamError{Code: 7, Reason: "reason only"}, "reason only"},
		{"带底层错误", &StreamError{Code: 7, Reason: "reason", Err: errors.New("boom")}, "reason: boom"},
		{"Reason 为空带底层错误", &StreamError{Err: errors.New("boom")}, ": boom"},
		{"全空", &StreamError{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.err.Error(); got != c.want {
				t.Errorf("Error() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestStreamErrorUnwrap(t *testing.T) {
	inner := errors.New("inner")
	e := &StreamError{Code: 1, Reason: "outer", Err: inner}

	if e.Unwrap() != inner {
		t.Fatal("Unwrap() 应返回底层错误")
	}
	if !errors.Is(e, inner) {
		t.Fatal("errors.Is 应能沿 Unwrap 找到底层错误")
	}
	if (&StreamError{Reason: "no inner"}).Unwrap() != nil {
		t.Fatal("无底层错误时 Unwrap() 应为 nil")
	}
}

// Is 的匹配维度是 Code，不是指针相等，也不是 Reason
func TestStreamErrorIs(t *testing.T) {
	cases := []struct {
		name   string
		e      *StreamError
		target error
		want   bool
	}{
		{"同 Code 不同实例", &StreamError{Code: 2, Reason: "x"}, &StreamError{Code: 2, Reason: "y"}, true},
		{"不同 Code", &StreamError{Code: 2}, &StreamError{Code: 3}, false},
		{"target 非 StreamError", &StreamError{Code: 2}, errors.New("x"), false},
		{"target 为 nil", &StreamError{Code: 2}, nil, false},
		{"target 是 typed nil", &StreamError{Code: 2}, (*StreamError)(nil), false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 直接调 Is，绕过 errors.Is 的 == 短路，才能测到方法本身
			if got := c.e.Is(c.target); got != c.want {
				t.Errorf("Is() = %v, want %v", got, c.want)
			}
			if got := errors.Is(c.e, c.target); got != c.want {
				t.Errorf("errors.Is() = %v, want %v", got, c.want)
			}
		})
	}
}

// nil 接收者只测 Is 方法本身：errors.Is 会在 Is 返回 false 后继续调用 Unwrap()，
// 而 *StreamError(nil).Unwrap() 会解引用 nil，那属于调用方式问题，不是 Is 的契约
func TestStreamErrorIsNilReceiver(t *testing.T) {
	var e *StreamError
	if e.Is(ErrRetryBackoff) {
		t.Error("nil 接收者的 Is 应返回 false")
	}
	if e.Is(nil) {
		t.Error("nil 接收者 + nil target 应返回 false")
	}
}

func TestSentinels(t *testing.T) {
	all := []*StreamError{
		ErrShuttingDown, ErrRetryNow, ErrRetryBackoff, ErrTokenExpired, ErrStopForOps,
	}

	seen := make(map[int]bool)
	for _, s := range all {
		if s == nil {
			t.Fatal("哨兵错误不应为 nil")
		}
		if s.Reason == "" {
			t.Errorf("Code=%d 的 Reason 为空", s.Code)
		}
		if s.Err != nil {
			t.Errorf("Code=%d 的哨兵不应携带底层错误", s.Code)
		}
		if seen[s.Code] {
			t.Errorf("Code=%d 重复", s.Code)
		}
		seen[s.Code] = true

		if !errors.Is(s, s) {
			t.Errorf("Code=%d 应能匹配自身", s.Code)
		}
	}

	// 不同类别之间不能互相匹配，否则 startAndBlock 的 switch 会走错分支
	for _, a := range all {
		for _, b := range all {
			if a.Code != b.Code && errors.Is(a, b) {
				t.Errorf("Code=%d 误匹配 Code=%d", a.Code, b.Code)
			}
		}
	}
}

func TestStreamErrorAs(t *testing.T) {
	inner := mongo.CommandError{Code: 286, Message: "history lost"}
	e := &StreamError{Code: ErrTokenExpired.Code, Reason: ErrTokenExpired.Reason, Err: inner}

	var se *StreamError
	if !errors.As(e, &se) {
		t.Fatal("errors.As 应能取到 *StreamError")
	}
	if se.Code != ErrTokenExpired.Code {
		t.Errorf("Code = %d, want %d", se.Code, ErrTokenExpired.Code)
	}

	var cmd mongo.CommandError
	if !errors.As(e, &cmd) {
		t.Fatal("errors.As 应能沿 Unwrap 取到 mongo.CommandError")
	}
	if cmd.Code != 286 {
		t.Errorf("CommandError.Code = %d, want 286", cmd.Code)
	}
}

// ================= AsStreamError =================

// invalidated 优先级最高：即使 err == nil、即使是 ctx 取消，也要立即重开
func TestAsStreamErrorInvalidatedPriority(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"err 为 nil", nil},
		{"err 为 ctx.Canceled", context.Canceled},
		{"err 为命令错误", mongo.CommandError{Code: 13, Message: "unauthorized"}},
		{"err 为普通错误", errors.New("network")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := AsStreamError(c.err, true)
			if got != ErrRetryNow {
				t.Fatalf("got %+v, want ErrRetryNow(Code=%d)", got, ErrRetryNow.Code)
			}
		})
	}
}

func TestAsStreamErrorNil(t *testing.T) {
	if got := AsStreamError(nil, false); got != nil {
		t.Fatalf("无错误时应返回 nil, got %+v", got)
	}
}

func TestAsStreamErrorContextCanceled(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"被包装的 Canceled", fmt.Errorf("watch stopped: %w", context.Canceled)},
		{"被包装的 DeadlineExceeded", fmt.Errorf("timeout: %w", context.DeadlineExceeded)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := AsStreamError(c.err, false)
			if got != ErrShuttingDown {
				t.Fatalf("got %+v, want ErrShuttingDown(Code=%d)", got, ErrShuttingDown.Code)
			}
		})
	}
}

func TestAsStreamErrorCommandErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		cmd  mongo.CommandError
		want *StreamError
	}{
		{"286 ChangeStreamHistoryLost", mongo.CommandError{Code: 286, Message: "history lost"}, ErrTokenExpired},
		{"13 Unauthorized", mongo.CommandError{Code: 13, Message: "unauthorized"}, ErrStopForOps},
		{"18 AuthenticationFailed", mongo.CommandError{Code: 18, Message: "auth failed"}, ErrStopForOps},

		// 源码里 case 2, 40415 的两个分支返回完全相同的 ErrStopForOps，
		// strings.Contains(..., "resume"/"token") 目前是死代码，此处把现状固化
		{"2 含 resume", mongo.CommandError{Code: 2, Message: "invalid resume token"}, ErrStopForOps},
		{"2 不含 resume", mongo.CommandError{Code: 2, Message: "bad value"}, ErrStopForOps},
		{"40415 含 token", mongo.CommandError{Code: 40415, Message: "token not found"}, ErrStopForOps},
		{"40415 不含 token", mongo.CommandError{Code: 40415, Message: "other"}, ErrStopForOps},

		{"6 HostUnreachable", mongo.CommandError{Code: 6}, ErrRetryBackoff},
		{"7 HostNotFound", mongo.CommandError{Code: 7}, ErrRetryBackoff},
		{"89 NetworkTimeout", mongo.CommandError{Code: 89}, ErrRetryBackoff},
		{"91 ShutdownInProgress", mongo.CommandError{Code: 91}, ErrRetryBackoff},
		{"262 ExceededTimeLimit", mongo.CommandError{Code: 262}, ErrRetryBackoff},
		{"9001 SocketException", mongo.CommandError{Code: 9001}, ErrRetryBackoff},
		{"43 CursorNotFound", mongo.CommandError{Code: 43}, ErrRetryBackoff},
		{"63 StaleShardVersion", mongo.CommandError{Code: 63}, ErrRetryBackoff},
		{"150 StaleEpoch", mongo.CommandError{Code: 150}, ErrRetryBackoff},
		{"13388 StaleConfig", mongo.CommandError{Code: 13388}, ErrRetryBackoff},
		{"10107 NotWritablePrimary", mongo.CommandError{Code: 10107}, ErrRetryBackoff},
		{"11600 InterruptedAtShutdown", mongo.CommandError{Code: 11600}, ErrRetryBackoff},
		{"11601 Interrupted", mongo.CommandError{Code: 11601}, ErrRetryBackoff},
		{"11602 InterruptedDueToReplStateChange", mongo.CommandError{Code: 11602}, ErrRetryBackoff},
		{"189 PrimarySteppedDown", mongo.CommandError{Code: 189}, ErrRetryBackoff},
		{"13435 NotPrimaryNoSecondaryOk", mongo.CommandError{Code: 13435}, ErrRetryBackoff},
		{"13436 NotPrimaryOrSecondary", mongo.CommandError{Code: 13436}, ErrRetryBackoff},

		{"未知 code 保守停服", mongo.CommandError{Code: 12345, Message: "unknown"}, ErrStopForOps},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := AsStreamError(c.cmd, false)
			assertCode(t, got, c.want, c.name)
			if got.Err == nil {
				t.Fatal("命令错误应被保留在 Err 字段中（便于排查）")
			}
			var cmd mongo.CommandError
			if !errors.As(got, &cmd) || cmd.Code != c.cmd.Code {
				t.Errorf("Err 中应能取到原 CommandError(Code=%d)", c.cmd.Code)
			}
		})
	}
}

// 4.4+ 的 ResumableChangeStreamError label：code 未命中 switch 时也要退避重试
func TestAsStreamErrorResumableLabel(t *testing.T) {
	cases := []struct {
		name   string
		labels []string
		want   *StreamError
	}{
		{"带 Resumable 标签", []string{"ResumableChangeStreamError"}, ErrRetryBackoff},
		{"标签在列表中靠后", []string{"OtherLabel", "ResumableChangeStreamError"}, ErrRetryBackoff},
		{"无标签", nil, ErrStopForOps},
		{"只有其他标签", []string{"NonResumable"}, ErrStopForOps},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cmd := mongo.CommandError{Code: 12345, Message: "unknown", Labels: c.labels}
			assertCode(t, AsStreamError(cmd, false), c.want, c.name)
		})
	}
}

// errors.As 必须能穿透包装，否则外层 fmt.Errorf 会把命令错误降级成"普通退避"
func TestAsStreamErrorWrappedCommandError(t *testing.T) {
	cmd := mongo.CommandError{Code: 286, Message: "history lost"}
	wrapped := fmt.Errorf("watch failed: %w", cmd)

	got := AsStreamError(wrapped, false)
	assertCode(t, got, ErrTokenExpired, "wrapped 286")

	var cmdGot mongo.CommandError
	if !errors.As(got, &cmdGot) || cmdGot.Code != 286 {
		t.Fatal("Err 中应能沿链取到原 CommandError")
	}
}

// 驱动/网络层的非命令错误统一退避，且原错误要保留
func TestAsStreamErrorPlainError(t *testing.T) {
	inner := errors.New("connection reset by peer")
	got := AsStreamError(inner, false)

	assertCode(t, got, ErrRetryBackoff, "plain error")
	if got.Err != inner {
		t.Fatal("Err 字段应保留原错误对象")
	}
	if got.Reason != ErrRetryBackoff.Reason {
		t.Errorf("Reason = %q, want %q", got.Reason, ErrRetryBackoff.Reason)
	}
	if got.Error() != fmt.Sprintf("%s: %v", ErrRetryBackoff.Reason, inner) {
		t.Errorf("Error() = %q", got.Error())
	}
}

// 非命令错误即使在 ctx 已取消、但错误本身不是 context.Canceled 时，仍按退避处理
func TestAsStreamErrorPlainErrorNotShutdown(t *testing.T) {
	got := AsStreamError(errors.New("cursor closed"), false)
	assertCode(t, got, ErrRetryBackoff, "cursor closed")
}
