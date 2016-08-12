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
    mapping.foreach((migrateeTable : MigrateeTable) => migrateeTable.readColumnsMetadata(session))

    mapping.foreach((migrateTable: MigrateeTable) => {
      if(migrateTable.primaryKeyNullControl() == false) {
        println("Primary Key can not be null, please check your tables and mappings")
        throw new Exception
      }
    })

    //create batch statements for each table
    for(i <- mapping) {
      //create batch statement
      var result : Any = ""
      var insert : String = "BEGIN BATCH "
      var batchCount: Int = 0
      var total: Int = 0

      val statement: Statement = new SimpleStatement("select * from " + i.mappedTableName)
      statement.setFetchSize(1000)

      var resultSet : ResultSet = session.execute(statement)
      var iterator = resultSet.iterator()

      var defaultInsertStatement : String = buildDefaultInsertStatement(i.tableName, i.columns)

      while(iterator.hasNext) {
        var row: Row = iterator.next()
        insert += defaultInsertStatement
        insert += i.findValuesOfColumns(row, session)
        insert = insert.substring(0,insert.size-1) //delete last comma
        insert += ");"

        batchCount += 1
        if(batchCount == 500) { //against batch statement too large error
          batchCount = 0
          insert += " APPLY BATCH";
          session.execute(insert)
          //println(total += batchCount)
          insert = "BEGIN BATCH "
        }
      }
      //run the batch statement
      insert += " APPLY BATCH;"
      session.execute(insert)
      println("Last Batch has finished")
    }
  }

  def buildDefaultInsertStatement(tableName: String, columns: mutable.Map[String, ColumnProperty]): String = {
    var dis: String = "INSERT INTO " + tableName + " ("

    columns.keySet.foreach((key: String) =>  dis += key + ",")
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