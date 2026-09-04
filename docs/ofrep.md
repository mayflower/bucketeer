# OpenFeature Remote Evaluation Protocol

Bucketeer implements OFREP 0.3 for vendor-neutral, server-side feature evaluation, including applications written in languages without a Bucketeer SDK. The base URL is the host of the API gRPC-gateway listener, not the Console or SPA host and not the legacy `/v1/gateway` path.

- `POST /ofrep/v1/evaluate/flags/{key}` evaluates one flag and records one evaluation event.
- `POST /ofrep/v1/evaluate/flags` evaluates every active flag with one shared context. It does not record evaluation events.

Both endpoints require an enabled Bucketeer `SDK_SERVER` API key. The standard OFREP forms are `X-API-Key: <key>` and `Authorization: Bearer <key>`. Bucketeer's existing raw `Authorization: <key>` form remains supported for compatibility. If multiple forms are supplied, they must identify the same key. Client SDK keys, public API keys, invalid keys, disabled keys, and keys for disabled environments are rejected. These endpoints are intended for trusted server applications; do not expose server keys in browsers or mobile applications.

Requests preserve `X-Request-ID` for log correlation and accept W3C Trace Context or B3 trace headers. If no request ID is supplied, Bucketeer generates one.

## Evaluation context

The request body must contain an object-valued `context` with a non-empty string `targetingKey`:

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

A successful single evaluation records one Bucketeer exposure event. Bulk responses, including `304`, record no exposures because populating a static provider cache does not prove that the application used any returned flag. Static bulk providers are therefore not exposure-safe for Bucketeer experiments.

## Direct request

```sh
curl --fail-with-body \
  -H "X-API-Key: ${BUCKETEER_SERVER_API_KEY}" \
  -H 'content-type: application/json' \
  --data '{"context":{"targetingKey":"user-123","country":"DE"}}' \
  "https://bucketeer.example.com/ofrep/v1/evaluate/flags/my-flag"
```

## Python OpenFeature provider

The community OFREP provider can call the single-evaluation endpoint. Version `0.3.0` was used for the compatibility smoke test.

```sh
python3 -m venv .venv
.venv/bin/pip install 'openfeature-provider-ofrep==0.3.0'
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

The published [Bucketeer OFREP OpenAPI description](../api-description/ofrep.openapi.yaml) is derived from the official OpenFeature protocol specification at commit `56d798eb9ee6608ca5554bdffe5f2b67c4e8bb10`. Response contract tests validate against this checked-in description.
