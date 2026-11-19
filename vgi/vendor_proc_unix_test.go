// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//go:build unix

package vgi_test

import (
	"os/exec"
	"syscall"
)

// startInGroup puts the fake vendor in its own process group, so stopping it
// also stops the interpreter a launcher such as uvx starts as its child.
func startInGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

func stopGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
