#!/usr/bin/env python3
# Copyright 2026 The cloud-spanner-emulator Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""On Linux, verify that gateway-only signals terminate and reap its backend."""

import argparse
import ctypes
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def check_shutdown(gateway, backend, shutdown_signal):
    grpc_port, http_port = free_port(), free_port()
    while http_port == grpc_port:
        http_port = free_port()
    child_pids = []
    with tempfile.TemporaryFile(mode="w+") as output:
        process = subprocess.Popen([
            gateway, "--grpc_binary=" + backend, "--hostname=127.0.0.1",
            "--grpc_port=" + str(grpc_port), "--http_port=" + str(http_port),
        ], stdout=output, stderr=output, start_new_session=True)
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Gateway exited before becoming ready")
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{http_port}/v1/projects/shutdown-test/instances",
                        timeout=1,
                    ):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise TimeoutError("Gateway did not become ready")
            child_pids = sorted({
                int(pid)
                for children in Path(f"/proc/{process.pid}/task").glob("*/children")
                for pid in children.read_text().split()
            })
            if len(child_pids) != 1:
                raise RuntimeError(f"Expected one backend, found {child_pids}")
            # Signal only the gateway; container teardown would hide orphaned children.
            process.send_signal(shutdown_signal)
            code = process.wait(timeout=15)
            if any(Path(f"/proc/{pid}").exists() for pid in child_pids):
                raise RuntimeError(f"{shutdown_signal.name} left the backend running or unreaped")
            if code != 0:
                raise RuntimeError(f"{shutdown_signal.name} returned gateway exit code {code}")
            for port in (grpc_port, http_port):
                with socket.socket() as sock:
                    if sock.connect_ex(("127.0.0.1", port)) == 0:
                        raise RuntimeError(f"Port {port} is still listening")
            print(f"{shutdown_signal.name}: gateway and backend exited; ports released.")
        except Exception:
            output.seek(0)
            print(output.read(), end="")
            raise
        finally:
            # Only clean up this test's process group, including an orphan from a failure.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=15)
            for pid in child_pids:
                try:
                    os.waitpid(pid, 0)
                except ChildProcessError:
                    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--backend", required=True)
    args = parser.parse_args()
    # Adopt failed-test orphans so cleanup can reap them even inside a dev container.
    PR_SET_CHILD_SUBREAPER = 36
    if ctypes.CDLL(None, use_errno=True).prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "Could not enable child subreaper")
    failures = []
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            check_shutdown(args.gateway, args.backend, shutdown_signal)
        except Exception as error:
            failures.append(str(error))
    if failures:
        raise RuntimeError("; ".join(failures))
