package plan

import (
	"errors"
	"fmt"
)

type ValidationError struct {
	Message string
	Hint    string
}

func (e *ValidationError) Error() string { return e.Message }

func verr(hint, format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...), Hint: hint}
}

func ValidationHint(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) && ve.Hint != "" {
		return ve.Hint
	}
	return ""
}
