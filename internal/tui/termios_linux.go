//go:build linux

package tui

import (
	"syscall"
	"unsafe"
)

// makeRaw switches the terminal behind fd into cbreak mode: canonical input
// processing and echo are disabled so key presses (including escape
// sequences such as arrow keys) are delivered to the program immediately
// instead of being line-buffered and echoed back onto the screen.
// It returns a restore function that reapplies the previous terminal state.
func makeRaw(fd uintptr) (func(), error) {
	var old syscall.Termios
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, syscall.TCGETS, uintptr(unsafe.Pointer(&old))); errno != 0 {
		return nil, errno
	}

	raw := old
	raw.Lflag &^= syscall.ICANON | syscall.ECHO
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, syscall.TCSETS, uintptr(unsafe.Pointer(&raw))); errno != 0 {
		return nil, errno
	}

	return func() {
		_, _, _ = syscall.Syscall(syscall.SYS_IOCTL, fd, syscall.TCSETS, uintptr(unsafe.Pointer(&old)))
	}, nil
}
