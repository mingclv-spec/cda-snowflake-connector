package com.mingclv.cda

import java.sql.Types
import org.scalatest.funsuite.AnyFunSuite

class SqlServerCurrentStateSpec extends AnyFunSuite {
  test("quotes safe SQL Server table and column identifiers") {
    assert(CurrentStateSql.quoteQualifiedTable("dbo.POLICY_CURRENT") == "[dbo].[POLICY_CURRENT]")
    assert(CurrentStateSql.quoteSqlServerIdentifier("PolicyID") == "[PolicyID]")
  }

  test("rejects SQL injection in table and column identifiers") {
    intercept[IllegalArgumentException] {
      CurrentStateSql.quoteQualifiedTable("dbo.POLICY; DROP TABLE X")
    }
    intercept[IllegalArgumentException] {
      CurrentStateSql.quoteSqlServerIdentifier("ID] DROP TABLE X--")
    }
  }

  test("derives a companion state table name") {
    assert(CurrentStateSql.stateTableName("dbo.POLICY_CURRENT") == "dbo.POLICY_CURRENT_CDA_STATE")
    assert(CurrentStateSql.stateTableName("POLICY_CURRENT") == "POLICY_CURRENT_CDA_STATE")
  }

  test("maps common Snowflake types to SQL Server types") {
    assert(CurrentStateSql.sqlServerType(ColumnInfo("ID", Types.NUMERIC, "NUMBER", 38, 0, false)) == "DECIMAL(38,0)")
    assert(CurrentStateSql.sqlServerType(ColumnInfo("ACTIVE", Types.BOOLEAN, "BOOLEAN", 1, 0, true)) == "BIT")
    assert(CurrentStateSql.sqlServerType(ColumnInfo("PAYLOAD", Types.OTHER, "VARIANT", 0, 0, true)) == "NVARCHAR(MAX)")
    assert(CurrentStateSql.sqlServerType(ColumnInfo("CREATED_AT", Types.TIMESTAMP, "TIMESTAMP_NTZ", 0, 9, true)) == "DATETIME2")
  }

  test("compares fixed-width sequence values in lexical order") {
    assert(CurrentStateSql.compareSequence("0002", "0001") > 0)
    assert(CurrentStateSql.compareSequence("0001", "0001") == 0)
    assert(CurrentStateSql.compareSequence("0001", "0002") < 0)
  }
}
