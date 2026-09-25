//go:build !linux && !darwin

package tui

import "os"

// enableRawMode is a no-op on platforms without termios ioctl support.
func enableRawMode(f *os.File) (restore func(), supported bool) {
	return nil, false
}
