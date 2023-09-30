package canyon

import "errors"

type ErrorWithRetryAfter struct {
	Err        error
	RetryAfter int32
}

func WrapRetryAfter(err error, retryAfter int32) error {
	return &ErrorWithRetryAfter{
		Err:        err,
		RetryAfter: retryAfter,
	}
}

func (e *ErrorWithRetryAfter) Error() string {
	return e.Err.Error()
}

func (e *ErrorWithRetryAfter) Unwrap() error {
	return e.Err
}

func ErrorHasRetryAfter(err error) (int32, bool) {
	var e *ErrorWithRetryAfter
	if errors.As(err, &e) {
		return e.RetryAfter, true
	}
	return 0, false
}
