# Architecture notes

## Processing flow

CDA manifest -> manifest parser -> file validation -> ingestion ledger -> Snowflake adapter -> load verification

## Open design decisions

1. Manifest schema: confirm actual field names and whether paths are S3 URIs, S3 keys, or relative to a prefix.
2. Ingestion mode:
   - COPY INTO / Snowpipe is a natural fit when the source artifact is already a Parquet file in object storage.
   - Snowpipe Streaming is a row/channel ingestion API; decide whether a Parquet reader should convert records into supported rows.
3. Target shape: raw VARIANT landing table versus typed relational tables.
4. CDA change-data semantics: confirm how deletes, sequence values, timestamps, and per-table ordering are represented in the manifest and Parquet metadata.
5. Idempotency: persist manifest/file identity and Snowflake load history; safely retry failed files.
6. Security: configure external stage integration and Snowflake authentication outside source code.

## Suggested next implementation milestones

- Add fixtures using a redacted real CDA manifest.
- Implement exact CDA schema parsing and unit tests.
- Add Snowflake configuration via environment variables / secret manager.
- Implement the selected loader and a durable ingestion ledger.
- Add integration tests in a non-production Snowflake schema.
- Add structured logs, metrics, retries, and CI.
