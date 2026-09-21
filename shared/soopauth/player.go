package soopauth

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrLoginRequired = errors.New("soop authentication required")
var ErrPlayerUnknown = errors.New("soop player response unrecognized")
var ErrOffline = errors.New("soop broadcast offline")

// LiveResult only treats explicit RESULT=0 as offline. -6 is login-required.
// Missing RESULT is accepted only with nonempty CHATNO for older responses.
func LiveResult(raw json.RawMessage, chatNo string) (bool, error) {
	if len(raw) == 0 {
		if chatNo != "" {
			return true, nil
		}
		return false, ErrPlayerUnknown
	}
	value := strings.Trim(string(raw), `"`)
	code, err := strconv.Atoi(value)
	if err != nil {
		return false, ErrPlayerUnknown
	}
	switch code {
	case 0:
		if chatNo != "" {
			return false, ErrPlayerUnknown
		}
		return false, nil
	case 1:
		if chatNo == "" {
			return false, ErrPlayerUnknown
		}
		return true, nil
	case -6:
		return false, ErrLoginRequired
	default:
		return false, fmt.Errorf("%w (code=%d)", ErrPlayerUnknown, code)
	}
}
