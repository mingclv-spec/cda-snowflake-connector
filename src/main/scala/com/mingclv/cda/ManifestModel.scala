package com.mingclv.cda

final case class ManifestFile(path: String, sizeBytes: Option[Long] = None)
final case class CdaManifest(files: Seq[ManifestFile], manifestId: Option[String] = None)
