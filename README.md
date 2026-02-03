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

Example config:

```json
{
  "api_token": "YOUR_API_TOKEN",
  "tenant_name": "your-tenant",
  "start_date": "2024-01-01T00:00:00Z"
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
- Base URL is derived from `tenant_name` as `https://{tenant_name}.aptem.co.uk/odata/1.0`.
- Pagination uses `@odata.nextLink` when present, otherwise `$top` and server defaults.
