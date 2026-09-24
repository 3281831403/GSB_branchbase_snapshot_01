//go:build !linux && !darwin

package tui

import "errors"

// makeRaw is a no-op on platforms without termios ioctl support; the TUI
// keeps working with line-buffered input there.
func makeRaw(fd uintptr) (func(), error) {
	return nil, errors.New("raw terminal mode unsupported on this platform")
}
