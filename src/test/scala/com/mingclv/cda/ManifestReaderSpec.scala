package com.mingclv.cda

import org.scalatest.funsuite.AnyFunSuite

class ManifestReaderSpec extends AnyFunSuite {
  test("rejects unsupported URI scheme") {
    val reader = new ManifestReader()
    val ex = intercept[IllegalArgumentException] {
      reader.read("https://example.invalid/manifest.json")
    }
    assert(ex.getMessage.contains("Unsupported manifest URI scheme"))
  }
}
