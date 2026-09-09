# tap-aptem

Singer tap for the Aptem OData API, built with the Meltano Singer SDK.

This tap discovers streams dynamically by reading the OData `$metadata` endpoint,
then builds schemas and stream definitions from the exposed entity sets.

## Configuration

Required:
- `api_token`: API token for the Aptem OData API.
- `tenant_name`: Aptem tenant name used to build the base URL.

Optional:
- `start_date`: RFC3339 timestamp used for incremental replication.
- `odata_version`: Aptem OData API version to discover and query, e.g. `"1.0"`
  (the default) or `"2.0"`. Aptem publishes different entities on different
  versions (e.g. `Actions` and `CheckpointAssessments` are only on `2.0`), so a
  tenant with data on both versions needs one tap-aptem instance configured per
  version.

Example config:

```json
{
  "api_token": "YOUR_API_TOKEN",
  "tenant_name": "your-tenant",
  "start_date": "2024-01-01T00:00:00Z"
}
```

Example config for the OData v2 feed:

```json
{
  "api_token": "YOUR_API_TOKEN",
  "tenant_name": "your-tenant",
  "odata_version": "2.0"
}
```

## Usage

```bash
tap-aptem --config config.json --discover
```

```bash
tap-aptem --config config.json --catalog catalog.json
```

## Notes

- Stream schemas are generated from `$metadata` at discovery time.
- Base URL is derived from `tenant_name` and `odata_version` as
  `https://{tenant_name}.aptem.co.uk/odata/{odata_version}`.
- Pagination uses `@odata.nextLink` when present, otherwise `$top` and server defaults.
