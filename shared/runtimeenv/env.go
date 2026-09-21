// Package runtimeenv loads the role's mounted secret file without invoking a shell.
package runtimeenv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var key = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func Load() error {
	path := os.Getenv("ROLE_ENV_FILE")
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("role environment file unavailable")
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found || !key.MatchString(name) {
			return errors.New("invalid role environment assignment")
		}
		if _, exists := os.LookupEnv(name); !exists {
			if err = os.Setenv(name, value); err != nil {
				return fmt.Errorf("could not load role setting %s", name)
			}
		}
	}
	if scanner.Err() != nil {
		return errors.New("role environment read failed")
	}
	return nil
}
