package com.mingclv.cda

import software.amazon.awssdk.services.s3.S3Client

object Main {
  def main(args: Array[String]): Unit = {
    val manifestArg = args.sliding(2).collectFirst {
      case Array("--manifest", value) => value
    }.getOrElse {
      System.err.println("Usage: sbt run -- --manifest <file://...|s3://bucket/key>")
      sys.exit(2)
    }

    val s3 = S3Client.create()
    try {
      val manifest = new ManifestReader(s3).read(manifestArg)
      println(s"Manifest ID: ${manifest.manifestId.getOrElse("(not provided)")}")
      println(s"Parquet files discovered: ${manifest.files.size}")
      manifest.files.foreach(f => println(s"  ${f.path}"))
      // Intentionally not marked as loaded. Wire a real adapter after agreeing
      // on the Snowflake stage, target schema and load semantics.
      new NotConfiguredSnowflakeIngestion().load(manifest)
    } finally s3.close()
  }
}
