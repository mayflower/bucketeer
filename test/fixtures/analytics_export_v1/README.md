# Analytics export contract v1 fixtures

Canonical response shapes for the endpoints described in
[docs/integrations/analytics-export-contract-v1.md](../../../docs/integrations/analytics-export-contract-v1.md).

These files are the shared reference between Bucketeer and a warehouse connector. A
connector checks its own copies against these, so a change to a Bucketeer response shape
fails the connector's fixture check instead of silently breaking a sync in production.

Every value here is invented. There are no production or customer values, no real
identifiers, and no credentials: `context.json` deliberately contains no API key, because
the endpoint it models never returns one.

The JSON field names are lowerCamelCase, matching what the gateway emits. Regenerate with:

```bash
TZ=UTC CGO_ENABLED=0 go test ./pkg/api/api/ -run TestAnalyticsExportFixtures -update
```

The generating test asserts these files match what the server's own response types
serialize, so they cannot drift from the server without a test failure.
