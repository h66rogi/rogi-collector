package model

// Platform represents a live streaming platform.
type Platform string

const (
	PlatformChzzk Platform = "chzzk"
	PlatformSoop  Platform = "soop"
	PlatformCime  Platform = "cime"
)

// IsValid returns true if the platform is a streaming platform.
func (p Platform) IsValid() bool {
	switch p {
	case PlatformChzzk, PlatformSoop, PlatformCime:
		return true
	default:
		return false
	}
}
