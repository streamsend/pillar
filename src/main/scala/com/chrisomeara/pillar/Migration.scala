package com.chrisomeara.pillar

import java.util.Date

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder

import scala.collection.mutable

object Migration {
  def apply(description: String, authoredAt: Date, up: Seq[String], mapping: Seq[MigrateeTable]): Migration = {
    new IrreversibleMigration(description, authoredAt, up, mapping)
  }

  def apply(description: String, authoredAt: Date, up: Seq[String], mapping: Seq[MigrateeTable], down: Option[Seq[String]]): Migration = {
    down match {
      case Some(downStatement) =>
        new ReversibleMigration(description, authoredAt, up, mapping, downStatement)
      case None =>
        new ReversibleMigrationWithNoOpDown(description, authoredAt, up, mapping)
    }
  }
}

trait Migration {
  val description: String
  val authoredAt: Date
  val up: Seq[String]
  val mapping: Seq[MigrateeTable]

  def key: MigrationKey = MigrationKey(authoredAt, description)

  def authoredAfter(date: Date): Boolean = {
    authoredAt.after(date)
  }

  def authoredBefore(date: Date): Boolean = {
    authoredAt.compareTo(date) <= 0
  }

  def executeUpStatement(session: Session) {
    up.foreach(session.execute)
    insertIntoAppliedMigrations(session)

    executeTableStatement(session)
  }

  def executeTableStatement(session: Session): Unit = {
    mapping.foreach((migrateeTable : MigrateeTable) => migrateeTable.readColumnNames(session))

    //create batch statements for each table
    for(i <- mapping) {
      //create batch statement
      var result : Any = ""
      var insert : String = "BEGIN BATCH "

      var resultSet : ResultSet = session.execute("select * from " + i.mappedTableName)
      var iterator = resultSet.iterator()

      var defaultInsertStatement : String = buildDefaultInsertStatement(i.tableName, i.tableColumnList)

      while(iterator.hasNext) {
        var row: Row = iterator.next()
        insert += defaultInsertStatement
        insert += i.findValuesOfColumns(row, session)
        insert = insert.substring(0,insert.size-1) //delete last comma
        insert += ");"
      }
      //run the batch statement
      insert += " APPLY BATCH;"
      session.execute(insert)
    }
  }

  def buildDefaultInsertStatement(tableName: String, columnNameList: mutable.MutableList[String]): String = {
    var dis: String = "INSERT INTO " + tableName + " ("

    columnNameList.foreach((columnName : String) => dis += columnName + ",")

    dis = dis.substring(0, dis.size - 1) //delete last comma
    dis += ") VALUES ("

    dis
  }

  def executeDownStatement(session: Session)

  protected def deleteFromAppliedMigrations(session: Session) {
    session.execute(QueryBuilder.
      delete().
      from("applied_migrations").
      where(QueryBuilder.eq("authored_at", authoredAt)).
      and(QueryBuilder.eq("description", description))
    )
  }

  private def insertIntoAppliedMigrations(session: Session) {
    session.execute(QueryBuilder.
      insertInto("applied_migrations").
      value("authored_at", authoredAt).
      value("description", description).
      value("applied_at", System.currentTimeMillis())
    )
  }
}

class IrreversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[MigrateeTable]) extends Migration {
  def executeDownStatement(session: Session) {
    throw new IrreversibleMigrationException(this)
  }
}

class ReversibleMigrationWithNoOpDown(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[MigrateeTable]) extends Migration {
  def executeDownStatement(session: Session) {
    deleteFromAppliedMigrations(session)
  }
}

class ReversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[MigrateeTable], val down: Seq[String]) extends Migration {
  def executeDownStatement(session: Session) {
    down.foreach(session.execute)
    deleteFromAppliedMigrations(session)
  }
}