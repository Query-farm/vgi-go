// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//go:build !unix

package vgi

import (
	"fmt"
	"os"
)

// checkPrivate is best effort off POSIX: the key lives in the user's profile
// temp directory.
func checkPrivate(path string, dir bool) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.IsDir() != dir {
		return fmt.Errorf("unexpected file type")
	}
	return nil
}
