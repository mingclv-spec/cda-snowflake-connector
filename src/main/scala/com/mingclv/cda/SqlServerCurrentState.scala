package com.mingclv.cda

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Types}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

final case class CurrentStateConfig(
  snowflakeJdbcUrl: String,
  snowflakeUser: String,
  snowflakePassword: String,
  snowflakeSourceTable: String,
  sqlServerJdbcUrl: String,
  sqlServerUser: String,
  sqlServerPassword: String,
  sqlServerTargetTable: String,
  primaryKeyColumns: Seq[String],
  sequenceColumn: String,
  operationColumn: String,
  deleteOperationValue: String,
  dryRun: Boolean
)

object CurrentStateConfig {
  private val Identifier = "^[A-Za-z_][A-Za-z0-9_$#]*(\\.[A-Za-z_][A-Za-z0-9_$#]*){0,2}$"
  private val Column = "^[A-Za-z_][A-Za-z0-9_$#]*$"

  def fromEnv(env: Map[String, String] = sys.env): CurrentStateConfig = {
    def required(k: String): String = env.get(k).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Required environment variable $k is not set"))
    def optional(k: String): Option[String] = env.get(k).map(_.trim).filter(_.nonEmpty)
    val keys = required("CDA_PRIMARY_KEY_COLUMNS").split(",").map(_.trim).toSeq
    val cfg = CurrentStateConfig(
      snowflakeJdbcUrl = required("SNOWFLAKE_JDBC_URL"),
      snowflakeUser = required("SNOWFLAKE_USER"),
      snowflakePassword = required("SNOWFLAKE_PASSWORD"),
      snowflakeSourceTable = required("SNOWFLAKE_CDC_SOURCE_TABLE"),
      sqlServerJdbcUrl = required("SQLSERVER_JDBC_URL"),
      sqlServerUser = required("SQLSERVER_USER"),
      sqlServerPassword = required("SQLSERVER_PASSWORD"),
      sqlServerTargetTable = required("SQLSERVER_CURRENT_TABLE"),
      primaryKeyColumns = keys,
      sequenceColumn = optional("CDA_SEQUENCE_COLUMN").getOrElse("gwcbi___seqval"),
      operationColumn = optional("CDA_OPERATION_COLUMN").getOrElse("gwcbi___operation"),
      deleteOperationValue = optional("CDA_DELETE_OPERATION_VALUE").getOrElse("1"),
      dryRun = optional("DRY_RUN").exists(_.equalsIgnoreCase("true"))
    )
    require(cfg.snowflakeSourceTable.matches(Identifier), "SNOWFLAKE_CDC_SOURCE_TABLE must be a qualified SQL identifier")
    require(cfg.sqlServerTargetTable.split("\\.").length <= 2 && cfg.sqlServerTargetTable.matches(Identifier), "SQLSERVER_CURRENT_TABLE must be table or schema.table")
    require(cfg.primaryKeyColumns.nonEmpty && cfg.primaryKeyColumns.forall(_.matches(Column)),
      "CDA_PRIMARY_KEY_COLUMNS must be a comma-separated list of simple column names")
    require(cfg.sequenceColumn.matches(Column) && cfg.operationColumn.matches(Column),
      "CDA sequence and operation columns must be simple column names")
    cfg
  }
}

/**
 * Applies a Snowflake CDC table to a SQL Server current-state table.
 *
 * Expected source contract: one row per CDC event, with business columns, key column(s),
 * a sequence column, and an operation column. The configured delete operation is treated
 * as a hard delete. A companion state table retains per-key sequence/tombstone state so
 * replaying old events cannot resurrect deleted rows.
 */
final class SqlServerCurrentStateSync(config: CurrentStateConfig) {
  import CurrentStateSql._

  def sync(): CurrentStateResult = {
    val snowProps = new Properties()
    snowProps.setProperty("user", config.snowflakeUser)
    snowProps.setProperty("password", config.snowflakePassword)
    val sqlProps = new Properties()
    sqlProps.setProperty("user", config.sqlServerUser)
    sqlProps.setProperty("password", config.sqlServerPassword)

    val snow = DriverManager.getConnection(config.snowflakeJdbcUrl, snowProps)
    try {
      val sourceSql = s"SELECT * FROM ${config.snowflakeSourceTable} ORDER BY ${config.sequenceColumn}"
      val sourceStmt = snow.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      try {
        sourceStmt.setFetchSize(1000)
        val rs = sourceStmt.executeQuery(sourceSql)
        try {
          val md = rs.getMetaData
          val sourceColumns = (1 to md.getColumnCount).map(i => md.getColumnLabel(i))
          val lowerToActual = sourceColumns.map(c => c.toLowerCase -> c).toMap
          val keys = config.primaryKeyColumns.map(k =>
            lowerToActual.getOrElse(k.toLowerCase, throw new IllegalArgumentException(s"Primary-key column '$k' not found in Snowflake source"))
          )
          val seqCol = lowerToActual.getOrElse(config.sequenceColumn.toLowerCase,
            throw new IllegalArgumentException(s"Sequence column '${config.sequenceColumn}' not found in Snowflake source"))
          val opCol = lowerToActual.getOrElse(config.operationColumn.toLowerCase,
            throw new IllegalArgumentException(s"Operation column '${config.operationColumn}' not found in Snowflake source"))
          val businessColumns = sourceColumns.filterNot(c =>
            c.equalsIgnoreCase(seqCol) || c.equalsIgnoreCase(opCol)
          )
          require(keys.forall(k => businessColumns.exists(_.equalsIgnoreCase(k))),
            "Primary-key columns must be business columns")

          val columns = (1 to md.getColumnCount).map { i =>
            ColumnInfo(md.getColumnLabel(i), md.getColumnType(i), md.getColumnTypeName(i),
              md.getPrecision(i), md.getScale(i), md.isNullable(i) != ResultSetMetaData.columnNoNulls)
          }
          val byName = columns.map(c => c.name.toLowerCase -> c).toMap
          val business = businessColumns.map(name => byName(name.toLowerCase))
          val keyInfos = keys.map(k => byName(k.toLowerCase))
          val target = config.sqlServerTargetTable
          val state = stateTableName(target)
          val createTarget = createTargetSql(target, business, keys)
          val createState = createStateSql(state, keyInfos, keys)
          if (config.dryRun) {
            return CurrentStateResult(0, 0, 0, "DRY_RUN",
              Seq(sourceSql, createTarget, createState, s"Upsert/delete events into $target using per-key sequence state in $state"))
          }

          val sqlServer = DriverManager.getConnection(config.sqlServerJdbcUrl, sqlProps)
          try {
            sqlServer.setAutoCommit(false)
            execute(sqlServer, createTarget)
            execute(sqlServer, createState)
            sqlServer.commit()
            var upserts = 0
            var deletes = 0
            var ignored = 0
            while (rs.next()) {
              val seq = Option(rs.getString(seqCol)).getOrElse(
                throw new IllegalArgumentException(s"Null CDC sequence in column '$seqCol'"))
              val op = Option(rs.getString(opCol)).getOrElse("")
              val values = business.map(c => rs.getObject(c.name))
              val keyValues = keys.map(k => values(business.indexWhere(_.name.equalsIgnoreCase(k))))
              require(keyValues.forall(_ != null), s"CDC row has a null primary key: ${keys.mkString(",")}")
              try {
                val previous = readState(sqlServer, state, keys, keyValues)
                if (previous.exists(p => compareSequence(seq, p) <= 0)) {
                  ignored += 1
                } else {
                  if (op == config.deleteOperationValue) {
                    deleteTarget(sqlServer, target, keys, keyValues)
                    deletes += 1
                  } else {
                    upsertTarget(sqlServer, target, business, keys, values, keyValues)
                    upserts += 1
                  }
                  writeState(sqlServer, state, keyInfos, keys, keyValues, seq,
                    deleted = op == config.deleteOperationValue)
                }
                sqlServer.commit()
              } catch {
                case e: Throwable =>
                  sqlServer.rollback()
                  throw e
              }
            }
            CurrentStateResult(upserts, deletes, ignored, "COMPLETED", Seq(s"State table: $state"))
          } finally sqlServer.close()
        } finally rs.close()
      } finally sourceStmt.close()
    } finally snow.close()
  }
}

final case class CurrentStateResult(upserts: Int, deletes: Int, ignoredOlderEvents: Int, status: String, details: Seq[String])

private[cda] final case class ColumnInfo(
  name: String, jdbcType: Int, typeName: String, precision: Int, scale: Int, nullable: Boolean
)

object CurrentStateSql {
  private val SimpleIdentifier = "^[A-Za-z_][A-Za-z0-9_$#]*$"

  def quoteSqlServerIdentifier(name: String): String = {
    require(name.matches(SimpleIdentifier), s"Unsafe SQL Server identifier: $name")
    s"[$name]"
  }

  def quoteQualifiedTable(name: String): String = {
    val parts = name.split("\\.")
    require(parts.length >= 1 && parts.length <= 2 && parts.forall(_.matches(SimpleIdentifier)),
      s"Table must be table or schema.table: $name")
    parts.map(quoteSqlServerIdentifier).mkString(".")
  }

  def stateTableName(target: String): String = {
    val parts = target.split("\\.")
    if (parts.length == 2) s"${parts(0)}.${parts(1)}_CDA_STATE" else s"${parts(0)}_CDA_STATE"
  }

  def sqlServerType(c: ColumnInfo): String = {
    val t = Option(c.typeName).getOrElse("").toUpperCase
    if (t.contains("VARIANT") || t.contains("OBJECT") || t.contains("ARRAY") || t == "GEOGRAPHY") "NVARCHAR(MAX)"
    else if (t.contains("TIMESTAMP") || t == "DATETIME" || t == "DATE") {
      if (t == "DATE") "DATE" else "DATETIME2"
    } else if (t == "BOOLEAN" || t == "BOOL") "BIT"
    else if (t == "BINARY") "VARBINARY(MAX)"
    else if (t == "NUMBER" || t == "FIXED" || t == "DECIMAL" || t == "NUMERIC") {
      val p = math.max(1, math.min(38, c.precision))
      val s = math.max(0, math.min(p, c.scale))
      s"DECIMAL($p,$s)"
    } else if (t == "INTEGER" || t == "INT" || t == "BIGINT") "BIGINT"
    else if (t == "FLOAT" || t == "DOUBLE" || t == "REAL") "FLOAT"
    else {
      val p = c.precision
      if (t.contains("CHAR") && p > 0 && p <= 4000) s"NVARCHAR($p)" else "NVARCHAR(MAX)"
    }
  }

  def createTargetSql(target: String, columns: Seq[ColumnInfo], keys: Seq[String]): String = {
    require(columns.nonEmpty, "Cannot create current-state table without business columns")
    val defs = columns.map(c => s"${quoteSqlServerIdentifier(c.name)} ${sqlServerType(c)}${if (keys.exists(_.equalsIgnoreCase(c.name))) " NOT NULL" else if (c.nullable) " NULL" else " NOT NULL"}")
    val pk = keys.map(quoteSqlServerIdentifier).mkString(", ")
    val qualified = quoteQualifiedTable(target)
    val table = target.split("\\.").last
    val schema = if (target.contains(".")) target.split("\\.")(0) else "dbo"
    s"IF OBJECT_ID(N'$schema.$table', N'U') IS NULL BEGIN CREATE TABLE $qualified (${(defs :+ s"CONSTRAINT ${quoteSqlServerIdentifier("PK_" + table.take(100))} PRIMARY KEY ($pk)").mkString(", ")}) END"
  }

  def createStateSql(state: String, keys: Seq[ColumnInfo], keyNames: Seq[String]): String = {
    val defs = keys.map(c => s"${quoteSqlServerIdentifier(c.name)} ${sqlServerType(c)} NOT NULL")
    val table = state.split("\\.").last
    val pk = keyNames.map(quoteSqlServerIdentifier).mkString(", ")
    val qualified = quoteQualifiedTable(state)
    val schema = if (state.contains(".")) state.split("\\.")(0) else "dbo"
    s"IF OBJECT_ID(N'$schema.$table', N'U') IS NULL BEGIN CREATE TABLE $qualified (${(defs ++ Seq("[_CDA_SEQVAL] NVARCHAR(256) NOT NULL", "[_CDA_DELETED] BIT NOT NULL", s"CONSTRAINT ${quoteSqlServerIdentifier("PK_" + table.take(100))} PRIMARY KEY ($pk)")).mkString(", ")}) END"
  }

  def compareSequence(a: String, b: String): Int = a.compareTo(b)

  def execute(c: Connection, sql: String): Unit = {
    val s = c.createStatement()
    try s.execute(sql) finally s.close()
  }

  private def keyWhere(keys: Seq[String]): String = keys.map(k => s"${quoteSqlServerIdentifier(k)} = ?").mkString(" AND ")

  private def bind(p: java.sql.PreparedStatement, values: Seq[Any]): Unit =
    values.zipWithIndex.foreach { case (v, i) => p.setObject(i + 1, v) }

  def readState(c: Connection, state: String, keys: Seq[String], keyValues: Seq[Any]): Option[String] = {
    val p = c.prepareStatement(s"SELECT [_CDA_SEQVAL] FROM ${quoteQualifiedTable(state)} WHERE ${keyWhere(keys)}")
    try {
      bind(p, keyValues)
      val rs = p.executeQuery()
      try if (rs.next()) Option(rs.getString(1)) else None finally rs.close()
    } finally p.close()
  }

  def deleteTarget(c: Connection, target: String, keys: Seq[String], keyValues: Seq[Any]): Unit = {
    val p = c.prepareStatement(s"DELETE FROM ${quoteQualifiedTable(target)} WHERE ${keyWhere(keys)}")
    try { bind(p, keyValues); p.executeUpdate() } finally p.close()
  }

  def upsertTarget(c: Connection, target: String, columns: Seq[ColumnInfo], keys: Seq[String],
                   values: Seq[Any], keyValues: Seq[Any]): Unit = {
    val nonKeys = columns.filterNot(c => keys.exists(_.equalsIgnoreCase(c.name)))
    val setClause = nonKeys.map(c => s"${quoteSqlServerIdentifier(c.name)} = ?").mkString(", ")
    val updateSql = if (setClause.nonEmpty) s"UPDATE ${quoteQualifiedTable(target)} SET $setClause WHERE ${keyWhere(keys)}" else ""
    val keyIndices = keys.map(k => columns.indexWhere(_.name.equalsIgnoreCase(k)))
    val pUpdateCount =
      if (updateSql.nonEmpty) {
        val p = c.prepareStatement(updateSql)
        try {
          val updateValues = nonKeys.map(col => values(columns.indexWhere(_.name.equalsIgnoreCase(col.name))))
          bind(p, updateValues ++ keyValues)
          p.executeUpdate()
        } finally p.close()
      } else 0
    if (pUpdateCount == 0) {
      val insertSql = s"INSERT INTO ${quoteQualifiedTable(target)} (${columns.map(c => quoteSqlServerIdentifier(c.name)).mkString(", ")}) VALUES (${columns.map(_ => "?").mkString(", ")})"
      val p = c.prepareStatement(insertSql)
      try { bind(p, values); p.executeUpdate() } finally p.close()
    }
  }

  def writeState(c: Connection, state: String, keyInfos: Seq[ColumnInfo], keys: Seq[String],
                 keyValues: Seq[Any], sequence: String, deleted: Boolean): Unit = {
    val previous = readState(c, state, keys, keyValues)
    previous match {
      case Some(_) =>
        val sql = s"UPDATE ${quoteQualifiedTable(state)} SET [_CDA_SEQVAL] = ?, [_CDA_DELETED] = ? WHERE ${keyWhere(keys)}"
        val p = c.prepareStatement(sql)
        try { p.setString(1, sequence); p.setBoolean(2, deleted); keyValues.zipWithIndex.foreach { case (v, i) => p.setObject(i + 3, v) }; p.executeUpdate() } finally p.close()
      case None =>
        val names = keys.map(quoteSqlServerIdentifier) ++ Seq("[_CDA_SEQVAL]", "[_CDA_DELETED]")
        val sql = s"INSERT INTO ${quoteQualifiedTable(state)} (${names.mkString(", ")}) VALUES (${names.map(_ => "?").mkString(", ")})"
        val p = c.prepareStatement(sql)
        try { bind(p, keyValues ++ Seq(sequence, java.lang.Boolean.valueOf(deleted))); p.executeUpdate() } finally p.close()
    }
  }
}
