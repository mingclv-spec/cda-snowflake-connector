# CDA Snowflake Connector

Manifest-driven ingestion for Guidewire Cloud Data Access (CDA) Parquet exports into Snowflake.

> **Status:** starter scaffold. Validate the exact CDA manifest schema and Snowflake stage/file-format conventions in your environment before production use.

## Design

1. Read a CDA manifest from a local file or S3.
2. Parse the manifest into a list of Parquet object paths.
3. Validate paths and reject unexpected file types.
4. Submit manifest entries to an ingestion adapter.
5. Track each manifest/file to support retries and idempotency.

The initial implementation separates manifest parsing from Snowflake ingestion. Snowflake file-based loading (`COPY INTO` / Snowpipe) is not the same mechanism as the Snowpipe Streaming SDK. Choose the ingestion adapter based on intended latency and file sizes.

## Prerequisites

- JDK 17
- sbt 1.9+
- AWS credentials configured through the AWS SDK default credential chain if reading from S3
- Snowflake role, warehouse, database, schema, external stage and Parquet file format configured by your administrator

## Build

```bash
sbt test
sbt run -- --manifest file:///path/to/manifest.json
```

For an S3 manifest:

```bash
sbt run -- --manifest s3://bucket/path/to/manifest.json
```

## Expected manifest shape

The CDA manifest schema can vary by producer/version. This scaffold expects a simple shape:

```json
{
  "files": [
    {"path": "s3://example-bucket/cda/table/part-00000.parquet"},
    {"path": "s3://example-bucket/cda/table/part-00001.parquet"}
  ]
}
```

Update `ManifestReader` to match the actual CDA manifest schema before using real exports.

## Ingestion strategies

- **Landing-table pattern:** load each Parquet row into a `VARIANT` column, preserving the source payload.
- **Typed-table pattern:** map Parquet fields to explicit Snowflake columns.
- **Snowpipe Streaming:** use the SDK's supported row/channel API; do not assume it accepts Parquet files as-is.

See `docs/architecture.md`.

## Security and reliability checklist

- Use IAM roles / workload identity where possible; never hard-code AWS keys.
- Use Snowflake key-pair authentication or a supported secret store.
- Keep an ingestion ledger keyed by manifest ID and file path.
- Retry transient failures with bounded exponential backoff.
- Do not mark a file successful until Snowflake confirms the load.
- Record row/file counts and error details; avoid logging secrets or sensitive row data.
- Define handling for deletes, duplicate files, late-arriving files and schema evolution.
