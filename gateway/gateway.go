//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Package gateway implements a REST gateway for the emulator GRPC service.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log"

	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	// We need this to make sure that the gateway can serialize the google.rpc.ErrorInfo proto.
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"

	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	lrgw "cloud_spanner_emulator/gateway/longrunning_operations_gateway"
	dagw "cloud_spanner_emulator/gateway/spanner_admin_database_gateway"
	iagw "cloud_spanner_emulator/gateway/spanner_admin_instance_gateway"
	spgw "cloud_spanner_emulator/gateway/spanner_gateway"
)

// Options encapsulates options for the emulator gateway.
type Options struct {
	GatewayAddress                                 string
	FrontendBinary                                 string
	FrontendAddress                                string
	CopyEmulatorStdout                             bool
	CopyEmulatorStderr                             bool
	LogRequests                                    bool
	EnableFaultInjection                           bool
	DisableQueryNullFilteredIndexCheck             bool
	OverrideMaxDatabasesPerInstance                int
	OverrideChangeStreamPartitionTokenAliveSeconds int
	RemoteFunctionsHostPort                        string
}

// Gateway implements the emulator gateway server.
type Gateway struct {
	opts Options
}

// New returns a new gateway.
func New(opts Options) *Gateway {
	return &Gateway{opts}
}

// Run starts the emulator gateway server.
func (gw *Gateway) Run() {
	// Start the emulator grpc server and redirect its output.
	emulatorArgs := []string{
		"--host_port", gw.opts.FrontendAddress,
	}
	if gw.opts.LogRequests {
		emulatorArgs = append(emulatorArgs, "--log_requests")
	}
	if gw.opts.EnableFaultInjection {
		emulatorArgs = append(emulatorArgs, "--enable_fault_injection")
	}
	if gw.opts.DisableQueryNullFilteredIndexCheck {
		emulatorArgs = append(emulatorArgs, "--disable_query_null_filtered_index_check")
	}
	if gw.opts.RemoteFunctionsHostPort != "" {
		if !strings.HasPrefix(gw.opts.RemoteFunctionsHostPort, "localhost:") {
			log.Fatal("Flag remote_functions_host_port must be `localhost:<port>`")
		}

		emulatorArgs = append(emulatorArgs, "--remote_functions_host_port", gw.opts.RemoteFunctionsHostPort)
	}
	emulatorArgs = append(emulatorArgs,
		fmt.Sprintf("--override_max_databases_per_instance=%d",
			gw.opts.OverrideMaxDatabasesPerInstance))
	emulatorArgs = append(emulatorArgs,
		fmt.Sprintf("--override_change_stream_partition_token_alive_seconds=%d",
			gw.opts.OverrideChangeStreamPartitionTokenAliveSeconds))

	cmd := exec.Command(gw.opts.FrontendBinary, emulatorArgs...)

	// Proxy emulator log to gateway log.
	if gw.opts.CopyEmulatorStdout {
		cmd.Stdout = os.Stdout
	}
	if gw.opts.CopyEmulatorStderr {
		cmd.Stderr = os.Stderr
	}

	// Handle shutdown before starting the backend so signals cannot orphan it
	// during startup.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Start the grpc server but won't block for the grpc server to be up.
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	// Terminate the grpc server if the gateway server is terminated.
	go func() {
		os.Exit(waitForEmulator(cmd, c))
	}()

	// Wait for the grpc server to be up.
	ctx := context.Background()
	addr := gw.opts.FrontendAddress
	if err = waitForReady(ctx, addr); err != nil {
		log.Fatal(fmt.Errorf("Error waiting for emulator to start: %v", err))
	}

	// Setup the gateway services.
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{}))
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err = spgw.RegisterSpannerHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}
	err = iagw.RegisterInstanceAdminHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}
	err = dagw.RegisterDatabaseAdminHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}
	err = lrgw.RegisterOperationsHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}

	// Start the gateway http server.
	log.Println("Cloud Spanner emulator running.")
	log.Println("REST server listening at", gw.opts.GatewayAddress)
	log.Println("gRPC server listening at", gw.opts.FrontendAddress)
	err = http.ListenAndServe(gw.opts.GatewayAddress, mux)
	if err != nil {
		log.Fatal(err)
	}
}

// waitForEmulator reaps the backend before the gateway exits, including when
// only the gateway receives a shutdown signal.
func waitForEmulator(cmd *exec.Cmd, signals <-chan os.Signal) int {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-signals:
		// Release must not precede Kill: Release makes the process unusable.
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			log.Printf("Terminating grpc server: %v", err)
		}
		<-done
		return 0
	case <-done:
		log.Println("Shutting down gateway server since grpc server is terminated.")
		if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() < 0 {
			return 1
		}
		return cmd.ProcessState.ExitCode()
	}
}

func waitForReady(ctx context.Context, endpoint string) error {
	timeout := 30 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	// To test whether the server is up, wait for ListInstanceConfigs to respond
	// for a dummy project.
	instanceAdminClient := instancepb.NewInstanceAdminClient(conn)
	if _, err = instanceAdminClient.ListInstanceConfigs(ctx,
		&instancepb.ListInstanceConfigsRequest{
			Parent: "projects/test-project",
		}); err != nil {
		return fmt.Errorf("emulator failed to come up at %v within %v deadline: %v",
			endpoint, timeout.String(), err)
	}
	return nil
}
