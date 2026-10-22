package com.mingclv.cda

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

/**
 * Loads manifest-listed Parquet files through a preconfigured Snowflake external stage.
 *
 * The stage URL must point at the configured S3 bucket/prefix. The role used by
 * Snowflake must have read access to that location. This connector does not move
 * files from S3; it asks Snowflake to read them directly.
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

final case class SnowflakeConfig(
  jdbcUrl: String,
  user: String,
  password: String,
  stage: String,
  targetTable: String,
  s3Bucket: String,
  s3Prefix: String,
  warehouse: Option[String],
  role: Option[String],
  fileFormatName: Option[String],
  dryRun: Boolean
)

object SnowflakeConfig {
  def fromEnv(env: Map[String, String] = sys.env): SnowflakeConfig = {
    def required(key: String): String =
      env.get(key).map(_.trim).filter(_.nonEmpty)
        .getOrElse(throw new IllegalArgumentException(s"Required environment variable $key is not set"))

    def optional(key: String): Option[String] =
      env.get(key).map(_.trim).filter(_.nonEmpty)

    val cfg = SnowflakeConfig(
      jdbcUrl = required("SNOWFLAKE_JDBC_URL"),
      user = required("SNOWFLAKE_USER"),
      password = required("SNOWFLAKE_PASSWORD"),
      stage = required("SNOWFLAKE_STAGE"),
      targetTable = required("SNOWFLAKE_TARGET_TABLE"),
      s3Bucket = required("CDA_S3_BUCKET"),
      s3Prefix = optional("CDA_S3_PREFIX").getOrElse("").stripPrefix("/").stripSuffix("/"),
      warehouse = optional("SNOWFLAKE_WAREHOUSE"),
      role = optional("SNOWFLAKE_ROLE"),
      fileFormatName = optional("SNOWFLAKE_FILE_FORMAT"),
      dryRun = optional("DRY_RUN").exists(_.equalsIgnoreCase("true"))
    )
    SnowflakeSql.validateIdentifier(cfg.stage.stripPrefix("@"), "SNOWFLAKE_STAGE")
    SnowflakeSql.validateIdentifier(cfg.targetTable, "SNOWFLAKE_TARGET_TABLE")
    cfg.fileFormatName.foreach(SnowflakeSql.validateIdentifier(_, "SNOWFLAKE_FILE_FORMAT"))
    cfg
  }
}

object SnowflakeSql {
  private val Identifier = "^[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$"

  def validateIdentifier(value: String, label: String): Unit =
    require(value.matches(Identifier), s"$label must be a simple or qualified Snowflake identifier")

  /** Resolve an S3 URI or key to a path relative to the configured external stage. */
  def stageRelativePath(filePath: String, bucket: String, prefix: String): String = {
    val key =
      if (filePath.startsWith("s3://")) {
        val rest = filePath.stripPrefix("s3://")
        val slash = rest.indexOf('/')
        require(slash > 0, s"Invalid S3 URI: $filePath")
        val actualBucket = rest.substring(0, slash)
        require(actualBucket == bucket, s"Manifest file bucket '$actualBucket' does not match CDA_S3_BUCKET")
        rest.substring(slash + 1)
      } else filePath.stripPrefix("/")

    val normalizedPrefix = prefix.stripPrefix("/").stripSuffix("/")
    val relative =
      if (normalizedPrefix.isEmpty) key
      else if (key == normalizedPrefix) ""
      else if (key.startsWith(normalizedPrefix + "/")) key.substring(normalizedPrefix.length + 1)
      else throw new IllegalArgumentException(s"File '$filePath' is outside configured CDA_S3_PREFIX '$normalizedPrefix'")

    require(relative.nonEmpty, s"File path resolves to an empty stage-relative path: $filePath")
    require(!relative.split("/").exists(p => p == "." || p == ".."), s"Unsafe relative path: $relative")
    require(relative.toLowerCase.endsWith(".parquet"), s"Not a Parquet file: $relative")
    require(relative.matches("^[A-Za-z0-9_./=+@ -]+$"), s"Unsupported characters in S3 key: $relative")
    relative
  }

  def copySql(stage: String, targetTable: String, relativePath: String, fileFormatName: Option[String]): String = {
    validateIdentifier(stage.stripPrefix("@"), "stage")
    validateIdentifier(targetTable, "target table")
    fileFormatName.foreach(validateIdentifier(_, "file format"))
    require(!relativePath.split("/").exists(p => p == "." || p == ".."), "Unsafe relative path")
    require(relativePath.matches("^[A-Za-z0-9_./=+@ -]+$"), "Unsupported characters in file path")
    val file = relativePath.replace("'", "''")
    val formatClause = fileFormatName match {
      case Some(name) => s" FILE_FORMAT = (FORMAT_NAME = '$name')"
      case None       => " FILE_FORMAT = (TYPE = PARQUET)"
    }
    s"COPY INTO $targetTable FROM $stage FILES = ('$file')$formatClause MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
  }
}

final class SnowflakeCopyIngestion(config: SnowflakeConfig) extends SnowflakeIngestion {
  override def load(manifest: CdaManifest): IngestionResult = {
    val paths = manifest.files.map(f =>
      SnowflakeSql.stageRelativePath(f.path, config.s3Bucket, config.s3Prefix)
    ).distinct

    val sqlStatements = paths.map(path =>
      SnowflakeSql.copySql(config.stage, config.targetTable, path, config.fileFormatName)
    )

    if (config.dryRun) {
      sqlStatements.foreach(sql => println(s"[DRY_RUN] $sql"))
      return IngestionResult(manifest.manifestId, paths.size, 0, "DRY_RUN", sqlStatements)
    }

    val props = new Properties()
    props.setProperty("user", config.user)
    props.setProperty("password", config.password)
    config.warehouse.foreach(props.setProperty("warehouse", _))
    config.role.foreach(props.setProperty("role", _))

    val connection = DriverManager.getConnection(config.jdbcUrl, props)
    try {
      val results = sqlStatements.map { sql =>
        val statement = connection.createStatement()
        try {
          val rs = statement.executeQuery(sql)
          try summarize(rs)
          finally rs.close()
        } finally statement.close()
      }
      val normalized = results.map(_.toUpperCase)
      val skipped = normalized.count(_.contains("STATUS=LOAD_SKIPPED"))
      val failed = normalized.exists(s => s.contains("STATUS=LOAD_FAILED") || s.contains("STATUS=PARTIALLY_LOADED"))
      IngestionResult(manifest.manifestId, results.size, skipped, if (failed) "FAILED" else "COMPLETED", results)
    } finally connection.close()
  }

  private def summarize(rs: ResultSet): String = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    while (rs.next()) {
      val cols = rs.getMetaData.getColumnCount
      val values = (1 to cols).map(i => s"${rs.getMetaData.getColumnLabel(i)}=${Option(rs.getString(i)).getOrElse("")}")
      out += values.mkString(", ")
    }
    out.mkString("; ")
  }
}
