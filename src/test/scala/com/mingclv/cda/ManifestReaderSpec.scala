package com.mingclv.cda

import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

class ManifestReaderSpec extends AnyFunSuite {
  test("rejects unsupported URI scheme") {
    val s3 = S3Client.builder().region(Region.US_EAST_1).build()
    try {
      val reader = new ManifestReader(s3)
      val ex = intercept[IllegalArgumentException] {
        reader.read("https://example.invalid/manifest.json")
      }
      assert(ex.getMessage.contains("Unsupported manifest URI scheme"))
    } finally s3.close()
  }
}
