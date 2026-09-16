package changestream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// StreamError 是 change stream 错误的具体类型。
// 用户通过 errors.Is 判断类别，通过 errors.As 拿到具体实例取 Code/Reason。
type StreamError struct {
	Code   int
	Reason string
	Err    error
}

func (e *StreamError) Error() string {
	if e.Err == nil {
		return e.Reason
	}
	return fmt.Sprintf("%s: %v", e.Reason, e.Err)
}

func (e *StreamError) Unwrap() error {
	return e.Err
}

func (e *StreamError) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}
	if se, ok := target.(*StreamError); ok {
		if se == nil {
			return false
		}
		return e.Code == se.Code
	}
	return false
}

// 哨兵错误，供 errors.Is 匹配
var (
	ErrShuttingDown = &StreamError{Code: 0, Reason: "shutting down"}
	ErrRetryNow     = &StreamError{Code: 1, Reason: "invalidate, reopen immediately"}
	ErrRetryBackoff = &StreamError{Code: 2, Reason: "transient error, backoff and retry"}
	ErrTokenExpired = &StreamError{Code: 3, Reason: "oplog window passed, token expired, must bootstrap"}
	ErrStopForOps   = &StreamError{Code: 4, Reason: "auth/config error, stop and wait for ops"}
)

// AsStreamError 将任意 error 转换为 *StreamError。
// invalidated 表示刚从事件流里读到了 operationType == "invalidate"。
// 返回 nil 表示无错误（正常继续）。
func AsStreamError(err error, invalidated bool) *StreamError {
	// invalidate 优先
	if invalidated {
		return ErrRetryNow
	}

	if err == nil {
		return nil
	}

	// ctx 取消
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrShuttingDown
	}

	// 服务端命令错误
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		return asCommandError(cmdErr)
	}

	// 非命令错误（驱动/网络层）→ 退避
	return &StreamError{
		Code:   ErrRetryBackoff.Code,
		Reason: ErrRetryBackoff.Reason,
		Err:    err,
	}
}

func asCommandError(cmdErr mongo.CommandError) *StreamError {
	switch cmdErr.Code {

	case 286: // ChangeStreamHistoryLost
		return &StreamError{
			Code:   ErrTokenExpired.Code,
			Reason: ErrTokenExpired.Reason,
			Err:    cmdErr,
		}

	case 13: // Unauthorized
		return &StreamError{
			Code:   ErrStopForOps.Code,
			Reason: ErrStopForOps.Reason,
			Err:    cmdErr,
		}

	case 18: // AuthenticationFailed
		return &StreamError{
			Code:   ErrStopForOps.Code,
			Reason: ErrStopForOps.Reason,
			Err:    cmdErr,
		}

	case 2, 40415: // BadValue / resume token 解析失败
		msg := strings.ToLower(cmdErr.Message)
		if strings.Contains(msg, "resume") || strings.Contains(msg, "token") {
			return &StreamError{
				Code:   ErrStopForOps.Code,
				Reason: ErrStopForOps.Reason,
				Err:    cmdErr,
			}
		}
		return &StreamError{
			Code:   ErrStopForOps.Code,
			Reason: ErrStopForOps.Reason,
			Err:    cmdErr,
		}

	// transient：退避重试
	case 6, 7, 89, 91, 262, 9001,
		43, 63, 150, 13388,
		10107, 11600, 11601, 11602, 189, 13435, 13436:
		return &StreamError{
			Code:   ErrRetryBackoff.Code,
			Reason: ErrRetryBackoff.Reason,
			Err:    cmdErr,
		}
	}

	// 4.4+ ResumableChangeStreamError label
	for _, label := range cmdErr.Labels {
		if label == "ResumableChangeStreamError" {
			return &StreamError{
				Code:   ErrRetryBackoff.Code,
				Reason: ErrRetryBackoff.Reason,
				Err:    cmdErr,
			}
		}
	}

	// 未识别 → 保守停服
	return &StreamError{
		Code:   ErrStopForOps.Code,
		Reason: ErrStopForOps.Reason,
		Err:    cmdErr,
	}
}
