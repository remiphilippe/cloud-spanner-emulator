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

"""Exercise mutation and constraint regressions through the REST gateway."""

import argparse
import json
import time
import urllib.error
import urllib.request


def smoke_test(endpoint):
    def request(method, path, body=None, expected_error=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            endpoint.rstrip("/") + "/v1/" + path,
            data=data,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                if expected_error is not None:
                    raise RuntimeError(f"Expected HTTP {expected_error}, got success")
                return json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            if error.code == expected_error:
                return json.loads(detail)
            raise RuntimeError(
                f"{method} {path} returned HTTP {error.code}: {detail}"
            ) from error

    def wait_operation(operation):
        for _ in range(60):
            if operation.get("done"):
                if "error" in operation:
                    raise RuntimeError(operation["error"])
                return
            time.sleep(1)
            operation = request("GET", operation["name"])
        raise TimeoutError("Emulator administration operation did not complete")

    last_error = None
    for _ in range(60):
        try:
            request("GET", "projects/image-smoke/instances")
            break
        except (OSError, RuntimeError) as error:
            last_error = error
            time.sleep(1)
    else:
        raise TimeoutError("Emulator REST/gRPC gateway did not become ready") from last_error

    instance = "projects/image-smoke/instances/smoke"
    database = instance + "/databases/fk-smoke"
    parent = "referenced_table_with_a_long_name_for_managed_index_regression"
    child = "referencing_table_with_a_long_name_for_managed_index_regression"
    column = "foreign_key_column_with_a_long_name_for_index_regression"

    wait_operation(request("POST", "projects/image-smoke/instances", {
        "instanceId": "smoke",
        "instance": {
            "config": "emulator-config", "displayName": "Image smoke test", "nodeCount": 1,
        },
    }))
    wait_operation(request("POST", instance + "/databases", {
        "createStatement": "CREATE DATABASE `fk-smoke`",
        "extraStatements": [
            f"CREATE TABLE {parent} (id INT64 NOT NULL, {column} INT64, extra INT64) "
            "PRIMARY KEY (id)",
            f"CREATE TABLE {child} (id INT64 NOT NULL, {column} INT64 NOT NULL, extra INT64, "
            f"CONSTRAINT composite_fk FOREIGN KEY ({column}, extra) "
            f"REFERENCES {parent} ({column}, extra)) PRIMARY KEY (id)",
            f"ALTER TABLE {child} ADD CONSTRAINT single_column_fk "
            f"FOREIGN KEY ({column}) REFERENCES {parent} ({column})",
            "CREATE TABLE update_defaults ("
            "id INT64 NOT NULL DEFAULT (1), "
            "status STRING(16) NOT NULL DEFAULT ('draft'), "
            "n INT64 NOT NULL DEFAULT (0), total INT64) PRIMARY KEY (id)",
        ],
    }))
    session = request("POST", database + "/sessions", {})["name"]
    result = request("POST", session + ":commit", {
        "singleUseTransaction": {"readWrite": {}},
        "mutations": [
            {"insert": {"table": table, "columns": ["id", column], "values": [["1", "10"]]}}
            for table in (child, parent)
        ],
    })
    if not result.get("commitTimestamp"):
        raise RuntimeError(f"Commit returned no timestamp: {result}")
    rows = request("POST", session + ":executeSql", {
        "sql": f"SELECT id, {column} FROM {child} ORDER BY id",
    })
    if rows.get("rows") != [["1", "10"]]:
        raise RuntimeError(f"Committed child row did not match: {rows}")
    print("Foreign-key regression passed through the release image's REST/gRPC gateway.")

    violation = request("POST", session + ":commit", {
        "singleUseTransaction": {"readWrite": {}},
        "mutations": [{"insert": {
            "table": child, "columns": ["id", column], "values": [["2", "999"]],
        }}],
    }, expected_error=400)
    if violation.get("code") != 9 or "foreign key" not in violation.get("message", "").lower():
        raise RuntimeError(f"REST did not preserve the FK error: {violation}")
    print("REST preserved the constraint violation's code and message.")

    for operation, columns, values in (
        ("insert", ["id", "status", "n", "total"], [["1", "sent", "5", "10"]]),
        ("update", ["id", "total"], [["1", "11"]]),
    ):
        request("POST", session + ":commit", {
            "singleUseTransaction": {"readWrite": {}},
            "mutations": [{operation: {
                "table": "update_defaults", "columns": columns, "values": values,
            }}],
        })
    rows = request("POST", session + ":executeSql", {
        "sql": "SELECT status, n, total FROM update_defaults WHERE id = 1",
    })
    if rows.get("rows") != [["sent", "5", "11"]]:
        raise RuntimeError(f"Partial update reset omitted columns: {rows}")
    print("Partial update preserved omitted defaulted columns.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Emulator REST endpoint")
    smoke_test(parser.parse_args().endpoint)
