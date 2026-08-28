# Analytics export contract v1

This document defines the stable, vendor-neutral contract that warehouse connectors use to
read Bucketeer configuration. It describes behavior that already exists in the public API,
plus the `GET /v1/export/context` endpoint added alongside this document.

The contract is versioned independently of the Bucketeer release. A connector reads
`contract_version` from `GET /v1/export/context` and refuses to run against a major version
it does not know.

## Stability promise

Within contract version `1`:

- listed endpoints keep their paths, list selectors and primary ID fields;
- the pagination envelope (`cursor`, `total_count`) keeps its meaning;
- timestamps stay in the units documented here;
- the privacy exclusions below stay excluded.

New response fields may appear at any time. Removing or renaming a documented field, or
changing cursor semantics, is a breaking change and requires a new contract version.

## Authentication

Send the environment API key as the raw value of the `Authorization` header. There is no
`Bearer` prefix and no other scheme:

```http
GET /v1/export/context
Authorization: <environment API key>
```

An environment API key is structurally bound to exactly one environment, so one connection
reads one environment. Reading several environments today means one connection per
environment. Organization-scoped export credentials are a later extension and are not part
of contract v1.

Export endpoints accept the `PUBLIC_API_READ_ONLY`, `PUBLIC_API_WRITE` and
`PUBLIC_API_ADMIN` roles. SDK keys are rejected. A disabled key or a disabled environment is
rejected even when the role is correct.

## Export context

`GET /v1/export/context` returns the non-secret context bound to the calling key:

```json
{
  "contractVersion": "1",
  "credentialScope": "environment",
  "organizationId": "org-...",
  "projectId": "prj-...",
  "projectUrlCode": "my-project",
  "environmentId": "env-...",
  "environmentName": "Production",
  "environmentUrlCode": "production",
  "capabilities": [
    "feature_flags",
    "segments",
    "experiments",
    "goals",
    "audit_logs",
    "code_references"
  ]
}
```

`credential_scope` is `environment` for an environment API key. `capabilities` lists only
resources this server exposes and tests; a connector must not walk an endpoint that is
absent from the list.

Every value is derived from the environment the key is bound to, never from the request. A
connector should treat this response as the authoritative lineage for every row it imports
and must not trust an environment field found inside a resource payload.

The response contains no API key, account, or user data.

## Endpoint matrix

| Capability | Path | Selector | ID field | Page size | Cursor in | Cursor out | Total count |
|---|---|---|---|---|---|---|---|
| `feature_flags` | `GET /v1/features` | `features` | `id` | `pageSize` | `cursor` | `cursor` | `totalCount` |
| `segments` | `GET /v1/segments` | `segments` | `id` | `pageSize` | `cursor` | `cursor` | `totalCount` |
| `experiments` | `GET /v1/experiments` | `experiments` | `id` | `pageSize` | `cursor` | `cursor` | `totalCount` |
| `goals` | `GET /v1/goals` | `goals` | `id` | `pageSize` | `cursor` | `cursor` | `totalCount` |
| `audit_logs` | `GET /v1/audit_logs` | `auditLogs` | `id` | `pageSize` (max 200) | `cursor` | `cursor` | `totalCount` |
| `code_references` | `GET /v1/code_references` | `codeReferences` | `id` | `pageSize` | `cursor` | `cursor` | `totalCount` |

`GET /v1/features` returned only `features` before this contract. It now returns the same
`cursor` and `total_count` envelope as the other endpoints, in fields 2 and 3 of
`ListFeaturesResponse`. Clients that read only `features` are unaffected.

## Pagination

- Request a bounded page size. Do not rely on `pageSize=0` to mean "everything".
- Send the `cursor` value from the previous response as the next request's `cursor`.
- An empty `cursor` in the response means there are no more pages, even when the page
  returned rows.
- `total_count` is the number of matching records, not the number of pages.
- A repeated cursor means the server did not advance. Fail the sync rather than loop.
- Ordering is not guaranteed to be stable across concurrent writes. A connector must
  deduplicate by primary key rather than assume a row appears exactly once per walk.

The gateway serializes responses with `EmitUnpopulated`, so an empty field is present rather
than omitted. An exhausted walk therefore arrives as `"cursor": ""`, not as a missing key.
Detect the end of pagination by an empty cursor value, not by the absence of the field.

JSON field names are lowerCamelCase (`totalCount`, `auditLogs`, `codeReferences`), while the
protobuf field names are snake_case. Both name the same field.

## Server-side filters

Only these filters are real. Do not synthesize an `updated_since` parameter for endpoints
that do not have one.

| Capability | Filter | Semantics |
|---|---|---|
| `audit_logs` | `from`, `to` | Inclusive bounds on the record `timestamp`, in Unix seconds. `order_by=TIMESTAMP` with `order_direction` is available. |
| `experiments` | `start_at`, `stop_at` | Experiment schedule bounds, in Unix seconds. These filter on the experiment window, not on when the row changed. |

No other listed capability exposes a server-side change-time filter. That is why a connector
should treat every other table as full refresh: the presence of a `created_at` or
`updated_at` field in a payload is not evidence that the server can filter on it.

## Timestamps

All timestamps in this contract are Unix seconds (UTC), as `int64`. They are not
milliseconds and not RFC 3339 strings.

## Errors

| Status | Meaning | Connector action |
|---|---|---|
| 401 | Missing, unknown, or expired key | Fail the connection; ask the user to re-enter the key |
| 403 | Valid key, insufficient role, or disabled key/environment | Fail that resource; report which capability was denied |
| 429 | Rate limited | Back off and retry; not evidence the table is forbidden |
| 5xx | Server error | Retry with backoff; not evidence the table is forbidden |

Distinguish 401/403 from 429/5xx. Treating a transient error as a permission error makes a
connector silently drop a table that the user can in fact read.

## Deletion under full refresh

A full refresh reflects deletions by absence: a record removed in Bucketeer stops appearing
in the list response. There is no tombstone and no deletion feed. A connector that appends
rather than replaces will retain deleted records indefinitely.

Archived records are a different case. Archived feature flags and goals are still returned,
carrying their archived flag, and are not deletions.

## Privacy exclusions

This contract exports configuration, not people. The following are out of scope and must not
be added to a `capabilities` list:

- accounts, users, and organization membership;
- segment member lists;
- API keys and any secret material;
- runtime evaluation and goal events.

Runtime events reach an analytics destination through Bucketeer's event pipeline, not
through this contract. A warehouse row from this contract describes configuration as it is
now; it is not an exposure record.

## Compatibility policy

- Adding a response field is compatible. A connector must ignore fields it does not know.
- Adding a capability is compatible.
- Removing or renaming a field, changing a selector, or changing cursor semantics is
  breaking and requires a new `contract_version`.
- Privacy exclusions cannot narrow within a major version: a field listed above as excluded
  cannot start being exported in a `1.x` server.

## Runtime event contract

Two event names belong to this contract. Their property names are as stable as the
warehouse field names above, because queries are written against them.

| Event | Emitted for |
|---|---|
| `bucketeer_feature_evaluated` | One flag evaluation |
| `bucketeer_goal_reached` | One goal conversion |

Both preserve the Bucketeer outer event id as the analytics event UUID and the original
event timestamp, so a redelivery is deduplicated rather than counted twice. Both set
`$process_person_profile` false and `$geoip_disable` true, and neither creates a person or
group profile.

`bucketeer_feature_evaluated` also sets `$feature_flag` and `$feature_flag_response` for
compatibility with flag-aware queries. The event keeps its own name: the assignment was
made by Bucketeer, not by the analytics tool.

## Checked-in fixtures

[`test/fixtures/analytics_export_v1/`](../../test/fixtures/analytics_export_v1/) holds the
canonical response shapes, and `events/` under it holds the two runtime events. They are
generated from the server's own response types and the real event mapper, so they cannot
drift from the implementation without failing a test.

A connector keeps its own copy and compares against these. Run the whole gate with:

```bash
make smoke-posthog
```

It needs no network, no credential and no running cluster.

Every value in those files is invented. Production and customer data must never be used
as a fixture, even with names replaced.

