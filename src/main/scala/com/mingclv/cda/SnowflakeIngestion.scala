package com.mingclv.cda

/**
 * Boundary between CDA manifest processing and a Snowflake ingestion strategy.
 *
 * Implement this interface with either:
 * - Snowflake COPY INTO / Snowpipe from an external stage; or
 * - Snowpipe Streaming SDK, after converting records to its supported row format.
 *
 * A Parquet object is not itself a Snowpipe Streaming row stream. The concrete
 * implementation must match the selected Snowflake ingestion mechanism.
 */
trait SnowflakeIngestion {
  def load(manifest: CdaManifest): IngestionResult
}

final case class IngestionResult(
  manifestId: Option[String],
  filesSubmitted: Int,
  filesSkipped: Int,
  status: String,
  details: Seq[String] = Seq.empty
)

/** Safe placeholder: refuses to claim data was loaded before an adapter exists. */
final class NotConfiguredSnowflakeIngestion extends SnowflakeIngestion {
  override def load(manifest: CdaManifest): IngestionResult =
    throw new UnsupportedOperationException(
      "Snowflake ingestion is not configured. Implement SnowflakeIngestion for your stage, file format, and target-table strategy."
    )
}
