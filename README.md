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


## Snowflake COPY INTO implementation

The connector now includes a JDBC-based `COPY INTO` loader for Parquet files already stored in S3. It expects a Snowflake external stage whose URL points at the same S3 bucket/prefix as the manifest entries. The Snowflake role must have the required stage and target-table privileges.

Set these environment variables before running:

| Variable | Required | Meaning |
|---|---|---|
| `SNOWFLAKE_JDBC_URL` | yes | JDBC URL, e.g. `jdbc:snowflake://<account>.snowflakecomputing.com` |
| `SNOWFLAKE_USER` | yes | Snowflake user |
| `SNOWFLAKE_PASSWORD` | yes | Password; use a secret manager in deployed environments |
| `SNOWFLAKE_STAGE` | yes | Stage name, e.g. `@RAW.CDA.CDA_STAGE` |
| `SNOWFLAKE_TARGET_TABLE` | yes | Destination table, e.g. `RAW.CDA.POLICY` |
| `CDA_S3_BUCKET` | yes | Bucket name expected in manifest S3 URIs |
| `CDA_S3_PREFIX` | no | Prefix configured on the stage URL; removed from manifest object keys |
| `SNOWFLAKE_WAREHOUSE` | no | Warehouse to use |
| `SNOWFLAKE_ROLE` | no | Role to use |
| `SNOWFLAKE_FILE_FORMAT` | no | Named Snowflake file format; otherwise uses `TYPE = PARQUET` |
| `DRY_RUN=true` | no | Print validated COPY statements without connecting to Snowflake |

Example:

```bash
export SNOWFLAKE_JDBC_URL='jdbc:snowflake://<account>.snowflakecomputing.com'
export SNOWFLAKE_USER='CDA_LOADER'
export SNOWFLAKE_PASSWORD='<from-secret-manager>'
export SNOWFLAKE_STAGE='@RAW.CDA.CDA_STAGE'
export SNOWFLAKE_TARGET_TABLE='RAW.CDA.POLICY'
export CDA_S3_BUCKET='my-cda-bucket'
export CDA_S3_PREFIX='exports'
export DRY_RUN='true'
sbt test
sbt run -- --manifest file:///absolute/path/manifest.json
```

For a manifest entry `s3://my-cda-bucket/exports/table/part-0001.parquet`, the stage must expose the `exports/` prefix and the generated `FILES` entry is `table/part-0001.parquet`.

### Important limitations before production

- The manifest parser currently expects `{"files":[{"path":"s3://.../*.parquet"}]}`; adapt it to the exact manifest schema emitted by your CDA version.
- The destination table must have compatible columns for `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`. Confirm the correct landing/typed-table design for your Parquet schema.
- This implementation submits one `COPY INTO` statement per file for precise manifest selection. For large manifests, batch files into bounded groups to reduce SQL round trips.
- Snowflake load history normally helps prevent reloading already-loaded files, but this is not a substitute for an application-level ingestion ledger or a plan for files changed in place.
- `DRY_RUN=true` is intended for statement review and does not verify connectivity, stage access, SQL validity or target schema.
- Username/password is supported as a starter. Prefer key-pair authentication or a supported secrets integration for production.
- This is not the Snowpipe Streaming SDK; it uses Snowflake SQL `COPY INTO` to load existing Parquet objects.

## SQL Server current-state CDC sync

The connector can apply CDC events already loaded in Snowflake to a SQL Server current-state table. Run it separately after the Snowflake load:

```bash
export SNOWFLAKE_CDC_SOURCE_TABLE='RAW.CDA.POLICY_CDC'
export SQLSERVER_JDBC_URL='jdbc:sqlserver://sqlserver-host:1433;databaseName=icare;encrypt=true;trustServerCertificate=false'
export SQLSERVER_USER='cda_sync'
export SQLSERVER_PASSWORD='set-this-through-your-secret-manager'
export SQLSERVER_CURRENT_TABLE='dbo.POLICY_CURRENT'
export CDA_PRIMARY_KEY_COLUMNS='ID'
export CDA_SEQUENCE_COLUMN='gwcbi___seqval'
export CDA_OPERATION_COLUMN='gwcbi___operation'
export CDA_DELETE_OPERATION_VALUE='1'
export DRY_RUN='true'
sbt run -- --sync-current-state
```

Required settings are `SNOWFLAKE_JDBC_URL`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_CDC_SOURCE_TABLE`, `SQLSERVER_JDBC_URL`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`, `SQLSERVER_CURRENT_TABLE`, and `CDA_PRIMARY_KEY_COLUMNS`. Sequence/operation column names and delete value can be overridden.

The source table must expose one row per CDC event, business columns, the configured key column(s), a sequence column, and an operation column. The implementation creates the SQL Server target and a companion `<target>_CDA_STATE` table when absent. The state table tracks each key's latest sequence and delete tombstone, making replayed events idempotent. Delete operations remove the row from the current table but retain the tombstone in the state table.

**Important assumptions to verify against your CDA export:** the source is a unified CDC event table; the operation column uses the configured delete value (default `1`); and sequence values are fixed-width strings whose lexical ordering matches CDA event ordering. If CDA stores deletes in a separate `_deletion` table, create a Snowflake view that normalizes change and deletion events into this contract before syncing. The connector does not yet infer primary keys or CDA schema automatically. Use a non-production SQL Server database first; live Snowflake/SQL Server integration is not exercised by unit CI.
