package connector

import (
	"fmt"
	"io"
)

const maxPlatformResponseBytes = 2 << 20

func readPlatformResponse(body io.Reader) ([]byte, error) {
	limited := io.LimitReader(body, maxPlatformResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > maxPlatformResponseBytes {
		return nil, fmt.Errorf("platform response exceeds %d bytes", maxPlatformResponseBytes)
	}
	return data, nil
}
