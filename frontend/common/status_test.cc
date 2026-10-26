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

#include "absl/status/status.h"

#include <string>

#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"
#include "absl/strings/cord.h"
#include "common/constants.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "frontend/common/status.h"
#include "googlesql/base/ret_check.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

TEST(GrpcStatusConversion, MessageLength) {
  std::string error_message(2048, 'a');
  EXPECT_THAT(ToGRPCStatus(absl::Status(absl::StatusCode::kInvalidArgument,
                                        error_message))
                  .error_message(),
              testing::EndsWith("..."));
}

TEST(GrpcStatusConversion, ConstraintMarkerIsNotSerializedAsProtoDetail) {
  absl::Status status = absl::FailedPreconditionError("Foreign key violation");
  status.SetPayload(kConstraintError, absl::Cord(""));
  google::rpc::ResourceInfo resource;
  resource.set_resource_name("test-table");
  status.SetPayload(kResourceInfoType,
                    absl::Cord(resource.SerializeAsString()));

  const grpc::Status grpc_status = ToGRPCStatus(status);
  EXPECT_EQ(grpc_status.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_EQ(grpc_status.error_message(), "Foreign key violation");
  google::rpc::Status details;
  ASSERT_TRUE(details.ParseFromString(grpc_status.error_details()));
  EXPECT_EQ(details.code(),
            static_cast<int>(absl::StatusCode::kFailedPrecondition));
  EXPECT_EQ(details.message(), "Foreign key violation");
  ASSERT_EQ(details.details_size(), 1);
  google::rpc::ResourceInfo decoded;
  ASSERT_TRUE(details.details(0).UnpackTo(&decoded));
  EXPECT_EQ(decoded.resource_name(), "test-table");
  EXPECT_TRUE(status.GetPayload(kConstraintError).has_value());
}

}  // namespace
}  // namespace emulator
}  // namespace spanner
}  // namespace google
