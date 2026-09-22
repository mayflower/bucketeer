#!/usr/bin/env python3
# Copyright 2026 The Bucketeer Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Smoke test: a community OpenFeature OFREP provider against Bucketeer.

The script drives the public OpenFeature client through
``openfeature-provider-ofrep`` (see requirements.txt) against the Bucketeer
API gateway, then checks a few protocol details the provider does not expose
with plain HTTP requests to the same base URL.

Configuration comes from environment variables:

  BUCKETEER_OFREP_BASE_URL    API gateway origin, e.g. https://api.example.com:9089
                              (the API host, not the console; no /v1/gateway path)
  BUCKETEER_SERVER_API_KEY    an enabled SDK_SERVER key for the fixture environment
  BUCKETEER_OFREP_FIXTURES    path to a JSON file describing pre-created flags:

    {
      "boolean": {
        "key": "flag-key",
        "match_context": {"attribute-1": "value-1", "attribute-2": "value-2"},
        "match_value": true,
        "other_value": false
      },
      "integer": {"key": "flag-key", "value": 42},
      "object":  {"key": "flag-key", "value": {"tier": "pro"}},
      "missing_key": "flag-key-that-does-not-exist"
    }

  REQUESTS_CA_BUNDLE          optional CA file for a local or test certificate.
                              TLS verification always stays enabled.

The script never creates or changes flags. The Go e2e suite
(test/e2e/gateway/ofrep_test.go) provisions the fixtures and runs this
script; see docs/mechanism/ofrep.md for the manual invocation.

Exit status is 0 only when every check ran against the endpoint and passed.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from typing import Any

import requests
from openfeature import api
from openfeature.contrib.provider.ofrep import OFREPProvider
from openfeature.evaluation_context import EvaluationContext
from openfeature.exception import ErrorCode
from openfeature.flag_evaluation import FlagEvaluationDetails, Reason

REQUEST_TIMEOUT_SECONDS = 5.0
TARGETING_KEY = "ofrep-smoke-user"


class CheckFailed(Exception):
    """A check ran and did not produce the expected outcome."""


def env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set; see the module docstring for the required environment")
    return value


def load_fixtures(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as handle:
            fixtures = json.load(handle)
    except (OSError, ValueError) as error:
        raise SystemExit(f"cannot read fixtures from {path}: {error}") from error
    for section in ("boolean", "integer", "object", "missing_key"):
        if section not in fixtures:
            raise SystemExit(f"fixtures file {path} is missing the {section!r} entry")
    return fixtures


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise CheckFailed(message)


def describe(details: FlagEvaluationDetails[Any]) -> str:
    return (
        f"value={details.value!r} reason={details.reason} variant={details.variant!r} "
        f"error_code={details.error_code} error_message={details.error_message!r}"
    )


def expect_remote_success(details: FlagEvaluationDetails[Any], expected: Any) -> None:
    """A returned default is not a successful remote evaluation."""
    expect(details.error_code is None, f"provider reported an error: {describe(details)}")
    expect(
        details.reason not in (Reason.ERROR, Reason.DEFAULT, None),
        f"evaluation did not come from the server: {describe(details)}",
    )
    expect(details.variant, f"evaluation has no variant: {describe(details)}")
    expect(details.value == expected, f"expected {expected!r}, got {describe(details)}")


def provider_for(base_url: str, headers: dict[str, str], domain: str):
    provider = OFREPProvider(
        base_url,
        headers_factory=lambda: dict(headers),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    api.set_provider_and_wait(provider, domain=domain)
    return api.get_client(domain=domain)


def check_typed_values(client, fixtures: dict[str, Any]) -> None:
    boolean = fixtures["boolean"]
    integer = fixtures["integer"]
    obj = fixtures["object"]
    plain = EvaluationContext(targeting_key=TARGETING_KEY)

    # Fallbacks deliberately differ from every expected value.
    details = client.get_boolean_details(boolean["key"], not boolean["other_value"], plain)
    expect_remote_success(details, boolean["other_value"])

    details = client.get_integer_details(integer["key"], integer["value"] + 1, plain)
    expect_remote_success(details, integer["value"])

    details = client.get_object_details(obj["key"], {"smoke": "fallback"}, plain)
    expect_remote_success(details, obj["value"])


def check_targeting(client, fixtures: dict[str, Any]) -> None:
    boolean = fixtures["boolean"]
    matching = EvaluationContext(targeting_key=TARGETING_KEY, attributes=dict(boolean["match_context"]))
    plain = EvaluationContext(targeting_key=TARGETING_KEY)

    matched = client.get_boolean_details(boolean["key"], not boolean["match_value"], matching)
    expect_remote_success(matched, boolean["match_value"])
    expect(matched.reason == Reason.TARGETING_MATCH, f"expected TARGETING_MATCH: {describe(matched)}")

    other = client.get_boolean_details(boolean["key"], not boolean["other_value"], plain)
    expect_remote_success(other, boolean["other_value"])
    expect(matched.value != other.value, "targeting context did not change the boolean result")


def check_missing_flag(client, fixtures: dict[str, Any]) -> None:
    details = client.get_boolean_details(fixtures["missing_key"], True, EvaluationContext(TARGETING_KEY))
    expect(details.error_code == ErrorCode.FLAG_NOT_FOUND, f"expected FLAG_NOT_FOUND: {describe(details)}")
    expect(details.value is True, f"missing flag must return the supplied fallback: {describe(details)}")
    expect(details.reason == Reason.ERROR, f"missing flag must carry the ERROR reason: {describe(details)}")


def check_bearer_scheme(bearer_client, fixtures: dict[str, Any]) -> None:
    integer = fixtures["integer"]
    details = bearer_client.get_integer_details(
        integer["key"], integer["value"] + 1, EvaluationContext(TARGETING_KEY)
    )
    expect_remote_success(details, integer["value"])


def post(session: requests.Session, url: str, headers: dict[str, str], body: dict[str, Any]) -> requests.Response:
    response = session.post(url, json=body, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type:
        raise CheckFailed(
            f"{url} answered with HTML (status {response.status_code}); "
            "BUCKETEER_OFREP_BASE_URL must be the API gateway, not the console"
        )
    return response


def check_http_details(session: requests.Session, base_url: str, api_key: str, fixtures: dict[str, Any]) -> None:
    body = {"context": {"targetingKey": TARGETING_KEY}}
    single = f"{base_url}/ofrep/v1/evaluate/flags/"
    bulk = f"{base_url}/ofrep/v1/evaluate/flags"
    auth = {"X-API-Key": api_key}

    response = post(session, single + fixtures["integer"]["key"], {"X-API-Key": "invalid-" + "x" * 8}, body)
    expect(response.status_code == 401, f"invalid key: expected 401, got {response.status_code}")
    challenge = response.headers.get("WWW-Authenticate", "")
    expect(challenge.startswith("Bearer"), f"invalid key: expected a Bearer challenge, got {challenge!r}")

    response = post(session, single + fixtures["missing_key"], auth, body)
    expect(response.status_code == 404, f"missing flag: expected 404, got {response.status_code}")
    expect(response.json().get("errorCode") == "FLAG_NOT_FOUND", f"missing flag: unexpected body {response.text!r}")

    first = post(session, bulk, auth, body)
    expect(first.status_code == 200, f"bulk: expected 200, got {first.status_code}")
    etag = first.headers.get("ETag", "")
    expect(etag.startswith('"'), f"bulk: expected a strong ETag, got {etag!r}")
    keys = {flag.get("key") for flag in first.json().get("flags", [])}
    expect(fixtures["integer"]["key"] in keys, "bulk: fixture flag missing from the bulk response")

    second = post(session, bulk, {**auth, "If-None-Match": etag}, body)
    expect(second.status_code == 304, f"bulk revalidation: expected 304, got {second.status_code}")
    expect(second.content == b"", "bulk revalidation: 304 must have an empty body")


def main() -> int:
    base_url = env("BUCKETEER_OFREP_BASE_URL").rstrip("/")
    api_key = env("BUCKETEER_SERVER_API_KEY")
    fixtures = load_fixtures(env("BUCKETEER_OFREP_FIXTURES"))

    api_key_client = provider_for(base_url, {"X-API-Key": api_key}, "x-api-key")
    bearer_client = provider_for(base_url, {"Authorization": f"Bearer {api_key}"}, "bearer")
    session = requests.Session()

    checks: list[tuple[str, Callable[[], None]]] = [
        ("typed values via X-API-Key", lambda: check_typed_values(api_key_client, fixtures)),
        ("targeting changes the boolean", lambda: check_targeting(api_key_client, fixtures)),
        ("missing flag returns fallback with FLAG_NOT_FOUND", lambda: check_missing_flag(api_key_client, fixtures)),
        ("Bearer scheme", lambda: check_bearer_scheme(bearer_client, fixtures)),
        ("HTTP 401 challenge, 404, and 304", lambda: check_http_details(session, base_url, api_key, fixtures)),
    ]
    failures = 0
    try:
        for name, check in checks:
            try:
                check()
                print(f"ok   - {name}")
            except CheckFailed as error:
                failures += 1
                print(f"FAIL - {name}: {error}")
            except requests.RequestException as error:
                failures += 1
                print(f"FAIL - {name}: request error: {error.__class__.__name__}: {error}")
    finally:
        api.shutdown()

    if failures:
        print(f"{failures} of {len(checks)} checks failed against {base_url}")
        return 1
    print(f"all {len(checks)} checks passed against {base_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
