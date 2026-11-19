// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//go:build !unix

package vgi_test

import "os/exec"

func startInGroup(*exec.Cmd) {}

func stopGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
