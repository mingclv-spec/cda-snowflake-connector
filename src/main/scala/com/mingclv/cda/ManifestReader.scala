package com.mingclv.cda

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io.{FileInputStream, InputStream}
import java.net.URI
import scala.collection.JavaConverters._

/**
 * Reads a manifest from file:// or s3://.
 * NOTE: Adapt the field mapping to the exact manifest emitted by your CDA version.
 */
final class ManifestReader(s3: Option[S3Client] = None) {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def read(uriText: String): CdaManifest = {
    val uri = new URI(uriText)
    val input = uri.getScheme match {
      case "file" => new FileInputStream(new java.io.File(uri))
      case "s3"   => openS3(uri)
      case other  => throw new IllegalArgumentException(
        s"Unsupported manifest URI scheme '$other'. Use file:// or s3://."
      )
    }
    try parse(input)
    finally input.close()
  }

  private def openS3(uri: URI): InputStream = {
    val bucket = uri.getHost
    val key = Option(uri.getPath).getOrElse("").stripPrefix("/")
    require(bucket != null && bucket.nonEmpty, "S3 URI must include a bucket")
    require(key.nonEmpty, "S3 URI must include an object key")
    val client = s3.getOrElse(throw new IllegalStateException("An S3Client must be supplied to read an s3:// manifest"))
    val response: software.amazon.awssdk.core.ResponseInputStream[software.amazon.awssdk.services.s3.model.GetObjectResponse] =
      client.getObject(
        GetObjectRequest.builder().bucket(bucket).key(key).build(),
        ResponseTransformer.toInputStream[software.amazon.awssdk.services.s3.model.GetObjectResponse]()
      )
    response
  }

  private def parse(input: InputStream): CdaManifest = {
    val root = mapper.readTree(input)
    val filesNode = root.path("files")
    require(filesNode.isArray, "Manifest must contain a files array (adapt parser to CDA schema)")
    val files = filesNode.elements().asScala.map { node =>
      val path = Option(node.get("path")).filterNot(_.isNull).map(_.asText()).getOrElse("")
      require(path.nonEmpty, "Each manifest file entry must contain a non-empty path")
      require(path.toLowerCase.endsWith(".parquet"), s"Not a Parquet path: $path")
      val size = Option(node.get("sizeBytes")).filter(_.canConvertToLong).map(_.asLong())
      ManifestFile(path, size)
    }.toSeq
    require(files.nonEmpty, "Manifest contains no Parquet files")
    val id = Option(root.get("manifestId")).filterNot(_.isNull).map(_.asText())
    CdaManifest(files, id)
  }
}
