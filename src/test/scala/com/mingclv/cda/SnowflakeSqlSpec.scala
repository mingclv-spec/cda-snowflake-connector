package com.mingclv.cda

import org.scalatest.funsuite.AnyFunSuite

class SnowflakeSqlSpec extends AnyFunSuite {
  test("maps an S3 URI to a path relative to the configured stage prefix") {
    assert(
      SnowflakeSql.stageRelativePath(
        "s3://cda-bucket/export/table/part-0001.parquet",
        "cda-bucket",
        "export"
      ) == "table/part-0001.parquet"
    )
  }

  test("rejects files from a different S3 bucket") {
    intercept[IllegalArgumentException] {
      SnowflakeSql.stageRelativePath("s3://other-bucket/a.parquet", "cda-bucket", "")
    }
  }

  test("rejects traversal paths") {
    intercept[IllegalArgumentException] {
      SnowflakeSql.stageRelativePath("s3://cda-bucket/export/../secret.parquet", "cda-bucket", "")
    }
  }

  test("generates COPY INTO SQL for Parquet") {
    val sql = SnowflakeSql.copySql(
      "@RAW.CDA.CDA_STAGE",
      "RAW.CDA.POLICY",
      "export/part-0001.parquet",
      None
    )
    assert(sql.contains("COPY INTO RAW.CDA.POLICY"))
    assert(sql.contains("FILES = ('export/part-0001.parquet')"))
    assert(sql.contains("FILE_FORMAT = (TYPE = PARQUET)"))
    assert(sql.contains("MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"))
  }

  test("rejects unsafe SQL identifiers") {
    intercept[IllegalArgumentException] {
      SnowflakeSql.copySql("@RAW.CDA.STAGE", "POLICY; DROP TABLE USERS", "part.parquet", None)
    }
  }
}
