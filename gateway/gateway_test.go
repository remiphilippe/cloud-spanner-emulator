// Copyright 2026 The cloud-spanner-emulator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gateway

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestEmulatorHelperProcess(t *testing.T) {
	mode := os.Getenv("SPANNER_GATEWAY_TEST_CHILD")
	if mode == "" {
		return
	}
	if mode == "exit" {
		os.Exit(23)
	}
	for {
		time.Sleep(time.Second)
	}
}

func startTestEmulator(t *testing.T, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestEmulatorHelperProcess$")
	cmd.Env = append(os.Environ(), "SPANNER_GATEWAY_TEST_CHILD="+mode)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return cmd
}

func TestWaitForEmulatorReapsChildOnSignal(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			cmd := startTestEmulator(t, "wait")
			signals := make(chan os.Signal, 1)
			signals <- sig
			result := make(chan int, 1)
			go func() { result <- waitForEmulator(cmd, signals) }()
			select {
			case code := <-result:
				if code != 0 {
					t.Fatalf("Exit code = %d, want 0", code)
				}
				if cmd.ProcessState == nil {
					t.Fatal("Backend was not reaped")
				}
				if err := cmd.Process.Signal(syscall.Signal(0)); err != os.ErrProcessDone {
					t.Fatalf("Backend still usable after shutdown: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Backend did not exit")
			}
		})
	}
}

func TestWaitForEmulatorPreservesChildExitCode(t *testing.T) {
	cmd := startTestEmulator(t, "exit")
	if code := waitForEmulator(cmd, make(chan os.Signal)); code != 23 {
		t.Fatalf("Exit code = %d, want 23", code)
	}
}
