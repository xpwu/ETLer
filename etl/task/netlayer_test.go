package task

import (
	"context"
	"errors"
	"testing"
)

func TestTypeString(t *testing.T) {
	cases := []struct {
		ty   Type
		want string
	}{
		{Sync, "sync"},
		{ChangeStream, "change-stream"},
		{Type(2), "<unknown>"},
		{Type(255), "<unknown>"},
	}
	for _, c := range cases {
		if got := c.ty.String(); got != c.want {
			t.Errorf("Type(%d).String() = %q, want %q", c.ty, got, c.want)
		}
	}
}

// Request.T 会以数字形式出现在下发给处理服务器的 JSON 中，
// 这两个常量的取值本身就是协议的一部分，不能随意调整
func TestTypeConstants(t *testing.T) {
	if Sync == ChangeStream {
		t.Fatal("Sync 与 ChangeStream 不能相同")
	}
	if Sync != 0 || ChangeStream != 1 {
		t.Fatalf("Sync=%d ChangeStream=%d, 期望 0/1", Sync, ChangeStream)
	}
}

func TestErrSendFailed(t *testing.T) {
	if ErrSendFailed == nil {
		t.Fatal("ErrSendFailed 不应为 nil")
	}
	if !errors.Is(ErrSendFailed, ErrSendFailed) {
		t.Fatal("errors.Is 应能匹配自身")
	}
	if errors.Is(ErrSendFailed, context.Canceled) {
		t.Fatal("ErrSendFailed 不应等价于 context.Canceled")
	}
}
