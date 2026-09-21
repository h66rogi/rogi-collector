package collection

import (
	"errors"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
)

func FailureState(err error) string {
	switch {
	case errors.Is(err, soopauth.ErrCookieUnavailable):
		return "cookie_required"
	case errors.Is(err, soopauth.ErrLoginRequired):
		return "auth_required"
	case errors.Is(err, soopauth.ErrOffline):
		return "waiting"
	default:
		return "lookup_failed"
	}
}
