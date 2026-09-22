# OpenFeature Remote Evaluation Protocol

Bucketeer implements OFREP 0.3 for vendor-neutral, server-side feature evaluation, including applications written in languages without a Bucketeer SDK. The base URL is the host of the API gRPC-gateway listener, not the Console or SPA host and not the legacy `/v1/gateway` path.

- `POST /ofrep/v1/evaluate/flags/{key}` evaluates one flag and queues one evaluation (exposure) event for asynchronous, best-effort delivery. The response never waits for the event broker.
- `POST /ofrep/v1/evaluate/flags` evaluates every active flag with one shared context. It does not record evaluation events.

Both endpoints require an enabled Bucketeer `SDK_SERVER` API key. The standard OFREP forms are `X-API-Key: <key>` and `Authorization: Bearer <key>`. Bucketeer's existing raw `Authorization: <key>` form remains supported for compatibility. If multiple forms are supplied, they must identify the same key. These endpoints are intended for trusted server applications; do not expose server keys in browsers or mobile applications.

| Credential outcome | Status |
| --- | --- |
| No credential, conflicting credentials, or a malformed `Bearer` value | `401` |
| Unknown key | `401` |
| Disabled key, or a key whose environment is disabled | `401` |
| Valid key without the `SDK_SERVER` role (client SDK or public API keys) | `403` |

`401` responses carry `WWW-Authenticate: Bearer realm="bucketeer-ofrep"`. A disabled environment is reported like a disabled key because Bucketeer's shared key check treats both as an unusable environment-bound credential. Native Bucketeer SDK endpoints keep their existing gRPC status codes.

Requests preserve `X-Request-ID` for log correlation and accept W3C Trace Context or B3 trace headers. If no request ID is supplied, Bucketeer generates one.

## Evaluation context

The request body is limited to 4 MiB, the same bound the gRPC server applies to the generated routes; larger bodies return `413`. It must contain an object-valued `context` with a non-empty string `targetingKey`:

```json
{
  "context": {
    "targetingKey": "user-123",
    "country": "DE",
    "subscribed": true,
    "profile": { "plan": "pro" }
  }
}
```

`targetingKey` becomes the Bucketeer user ID. Other string attributes are passed to the evaluator unchanged. Boolean, number, object, array, and null attributes are passed as compact JSON strings, matching Bucketeer's existing `map<string,string>` user attributes. Unknown attributes are preserved.

The response value follows the flag's Bucketeer variation type:

- `STRING` returns a JSON string.
- `BOOLEAN` returns a JSON boolean.
- `NUMBER` returns a JSON number without forcing integers through a floating-point conversion.
- `JSON` and `YAML` return a JSON object. Arrays, scalars, and malformed object values return `PARSE_ERROR`.

The response `variant` is the Bucketeer variation ID. Metadata contains `featureVersion`, `bucketeerReason`, and `ruleId` when a rule matched.

| Bucketeer result | OFREP reason |
| --- | --- |
| Target or prerequisite | `TARGETING_MATCH` |
| Off variation | `DISABLED` |
| Fixed targeting rule | `TARGETING_MATCH` |
| Rollout targeting rule | `SPLIT` |
| Fixed default strategy | `STATIC` |
| Rollout default strategy | `SPLIT` |
| Client or unknown reason | `UNKNOWN` |

Bulk results are sorted by flag key and omit archived flags. A malformed value affects only that flag; the remaining flags are still returned. The response does not advertise event streams. Its strong `ETag` is derived from the complete sorted JSON response. An exactly matching `If-None-Match` value, a matching comma-separated value, or `*` returns `304` with no body. Bucketeer does not add a separate OFREP response cache.

## Exposure events

A successful single evaluation builds one Bucketeer evaluation event, stamped with the evaluation time, and hands it to an in-process queue (1024 events, 4 publication workers). Each queued event gets one publication attempt with a 250 ms timeout through the configured event publisher. Delivery is best effort: an event is discarded when the queue is full, when the publisher rejects or times out, or when the process shuts down before the queue drains. Discards are counted in `bucketeer_gateway_api_ofrep_exposure_dropped_total` by reason (`queue_full`, `queue_closed`, `shutdown`); publication results use the existing `bucketeer_gateway_api_register_events_total` counter. A successful HTTP response therefore does not guarantee a recorded event, and a recorded event only shows that a server evaluated the flag, not that a user saw the result.

Bulk responses, including `304`, record no exposures because populating a static provider cache does not prove that the application used any returned flag. Static bulk providers are therefore not exposure-safe for Bucketeer experiments.

## Direct request

```sh
curl --fail-with-body \
  -H "X-API-Key: ${BUCKETEER_SERVER_API_KEY}" \
  -H 'content-type: application/json' \
  --data '{"context":{"targetingKey":"user-123","country":"DE"}}' \
  "https://bucketeer.example.com/ofrep/v1/evaluate/flags/my-flag"
```

## Python OpenFeature provider

The community OFREP provider (`openfeature-provider-ofrep` `0.3.0`, pinned in [`tools/ofrep/requirements.txt`](../../tools/ofrep/requirements.txt)) calls the single-evaluation endpoint through the public OpenFeature client.

```sh
python3 -m venv .venv
.venv/bin/pip install -r tools/ofrep/requirements.txt
```

```python
import os

from openfeature import api
from openfeature.contrib.provider.ofrep import OFREPProvider
from openfeature.evaluation_context import EvaluationContext

provider = OFREPProvider(
    "https://bucketeer.example.com",
    headers_factory=lambda: {
        "X-API-Key": os.environ["BUCKETEER_SERVER_API_KEY"]
    },
)
api.set_provider_and_wait(provider)
client = api.get_client()
context = EvaluationContext(
    targeting_key="user-123",
    attributes={"country": "DE"},
)

enabled = client.get_boolean_value("boolean-flag", False, context)
ratio = client.get_float_value("number-flag", 0.0, context)
config = client.get_object_value("object-flag", {}, context)
print(enabled, ratio, config)
```

The provider performs one remote request per flag evaluation. OFREP 0.3 has no goal or tracking endpoint. Bucketeer's implementation does not add SSE, browser credentials, proprietary tagging, flag management, CORS policy, rate limiting, retries, or other protocol extensions.

## Provider smoke test

[`tools/ofrep/ofrep_smoke_test.py`](../../tools/ofrep/ofrep_smoke_test.py) runs the provider above against a live API gateway and checks, through the OpenFeature client, that boolean, integer, and object flags return their expected values with successful evaluation details, that a targeting attribute changes the boolean result, that a missing flag returns the supplied fallback with `FLAG_NOT_FOUND`, and that `X-API-Key` and `Bearer` both authenticate. Plain HTTP requests to the same base URL then check the `401` challenge for an invalid key, `404` for a missing flag, and `304` with an empty body on bulk revalidation. An HTML answer or an SDK fallback is a failure; the script exits non-zero unless every check ran and passed. It never creates or changes flags.

The e2e suite provisions the fixture flags with its existing feature helpers and runs the script when `BUCKETEER_OFREP_SMOKE_PYTHON` names an interpreter with the requirements installed; otherwise `TestOFREPProviderSmoke` is skipped and reported as not run. Against a disposable deployment:

```sh
python3 -m venv .venv && .venv/bin/pip install -r tools/ofrep/requirements.txt
BUCKETEER_OFREP_SMOKE_PYTHON="$PWD/.venv/bin/python" \
  go test -v -run TestOFREPProviderSmoke ./test/e2e/gateway/ -args \
  -web-gateway-addr="$WEB_GATEWAY_URL" -web-gateway-cert="$WEB_GATEWAY_CERT_PATH" \
  -gateway-addr="$GATEWAY_URL" -gateway-port=443 -gateway-cert="$GATEWAY_CERT_PATH" \
  -api-key-server="$API_KEY_SERVER_PATH" -environment-id="$ENVIRONMENT_ID" \
  -org-owner-e2e-access-token="$ORG_OWNER_E2E_ACCESS_TOKEN_PATH" -test-id="$TEST_ID"
```

The same variable also enables the test inside `make e2e`. The gateway address decides what is exercised: the ingress host tests the deployed route, while the API listener (port `9089` by default) tests the service directly. To run the script by hand against flags you created yourself, set `BUCKETEER_OFREP_BASE_URL`, `BUCKETEER_SERVER_API_KEY`, and `BUCKETEER_OFREP_FIXTURES` (a JSON file in the shape documented at the top of the script), plus `REQUESTS_CA_BUNDLE` for a local certificate authority. TLS verification stays on.

The published [Bucketeer OFREP OpenAPI description](../../api-description/ofrep.openapi.yaml) is derived from the official OpenFeature protocol specification at commit `56d798eb9ee6608ca5554bdffe5f2b67c4e8bb10`. Response contract tests validate against this checked-in description.
