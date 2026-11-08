// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//go:build unix

package vgi

import (
	"fmt"
	"os"
	"syscall"
)

// checkPrivate refuses a key file or directory that another user owns or that
// group/other can access.
func checkPrivate(path string, dir bool) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.IsDir() != dir {
		return fmt.Errorf("unexpected file type")
	}
	if sys, ok := st.Sys().(*syscall.Stat_t); ok && int(sys.Uid) != os.Getuid() {
		return fmt.Errorf("owned by uid %d, not the current user (uid %d)", sys.Uid, os.Getuid())
	}
	if st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("mode %#o is group/other-accessible; want %#o", st.Mode().Perm(), map[bool]os.FileMode{true: 0o700, false: 0o600}[dir])
	}
	return nil
}
