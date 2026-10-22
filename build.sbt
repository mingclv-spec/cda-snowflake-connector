ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "com.mingclv"
ThisBuild / version := "0.2.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "cda-snowflake-connector",
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.3",
      "software.amazon.awssdk" % "s3" % "2.30.31",
      "net.snowflake" % "snowflake-jdbc" % "3.24.2",
      "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11",
      "org.slf4j" % "slf4j-simple" % "2.0.16",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Compile / mainClass := Some("com.mingclv.cda.Main")
  )
