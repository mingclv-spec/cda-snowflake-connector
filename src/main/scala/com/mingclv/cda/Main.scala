package com.mingclv.cda

import software.amazon.awssdk.services.s3.S3Client

object Main {
  def main(args: Array[String]): Unit = {
    val options = args.sliding(2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap

    val manifestUri = options.getOrElse("manifest", {
      System.err.println("Usage: sbt run -- --manifest <file://...|s3://bucket/key>")
      sys.exit(2)
    })

    val s3 = S3Client.create()
    try {
      val manifest = new ManifestReader(Some(s3)).read(manifestUri)
      println(s"Manifest ID: ${manifest.manifestId.getOrElse("(not provided)")}")
      println(s"Parquet files discovered: ${manifest.files.size}")
      manifest.files.foreach(f => println(s"  ${f.path}"))

      val config = SnowflakeConfig.fromEnv()
      val result = new SnowflakeCopyIngestion(config).load(manifest)
      println(s"Load status: ${result.status}")
      println(s"Files submitted: ${result.filesSubmitted}")
      println(s"Files skipped: ${result.filesSkipped}")
      result.details.foreach(println)
      if (result.status == "FAILED") sys.exit(1)
    } finally s3.close()
  }
}
