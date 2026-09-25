//go:build linux || darwin

package tui

import (
	"os"
	"syscall"
	"unsafe"
)

// enableRawMode switches the terminal backing f into raw mode: input is
// delivered byte-by-byte without line buffering and is not echoed back to the
// screen, so escape sequences (e.g. arrow keys) never leak into the UI.
// It returns a restore function and whether raw mode was enabled. If f is not
// a terminal, it returns supported=false and leaves the file descriptor
// untouched.
func enableRawMode(f *os.File) (restore func(), supported bool) {
	fd := f.Fd()

	var original syscall.Termios
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, ioctlGetTermios, uintptr(unsafe.Pointer(&original))); errno != 0 {
		return nil, false
	}

	raw := original
	raw.Iflag &^= syscall.ICRNL | syscall.IXON
	raw.Lflag &^= syscall.ECHO | syscall.ICANON | syscall.IEXTEN | syscall.ISIG
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, ioctlSetTermios, uintptr(unsafe.Pointer(&raw))); errno != 0 {
		return nil, false
	}

	return func() {
		_, _, _ = syscall.Syscall(syscall.SYS_IOCTL, fd, ioctlSetTermios, uintptr(unsafe.Pointer(&original)))
	}, true
}
