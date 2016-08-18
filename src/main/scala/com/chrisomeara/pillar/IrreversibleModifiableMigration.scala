package com.chrisomeara.pillar

import java.util.Date

import com.chrisomeara.pillar.modify.CqlStrategy
import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by mgunes on 16.08.2016.
  */
class IrreversibleModifiableMigration(val description: String, val authoredAt: Date, val fetch1: String, val up: Seq[String], val mapping1: Seq[MigrateeTable]) extends Migration {
  val fetch: String = fetch1
  val mapping: Seq[MigrateeTable] = mapping1

  def primaryKeyNullControl(): Unit = {
    mapping.foreach((migrateTable: MigrateeTable) => {
      if(!migrateTable.primaryKeyNullControl()) {
        println("Primary Key can not be null, please check your tables and mappings")
        throw new Exception
      }
    })
  }

  def eager(session: Session, fetchLimit: Int): Unit = {
    mapping.foreach((migrateeTable: MigrateeTable) => {
      migrateeTable.columns.keySet.foreach((key: String) => {
        if(migrateeTable.columns(key).modifyOperation.isInstanceOf[CqlStrategy])
          migrateeTable.columns(key).modifyOperation = CqlStatement.createCqlStrategy(migrateeTable, session, key, fetchLimit)
      })
    })
  }

  override def executeTableStatement(session: Session): Unit = {
    val config = ConfigFactory.load()
    val batchLimit: Int = config.getInt("cassandra-batch-size")
    val fetchLimit: Int = config.getInt("cassandra-fetch-size")

    mapping.foreach((migrateeTable: MigrateeTable) => migrateeTable.readColumnsMetadata(session))

    if(fetch.equalsIgnoreCase("eager"))
      eager(session, fetchLimit)

    primaryKeyNullControl()

    //create batch statements for each table
    for(i <- mapping) {
      //create batch statement
      val insert = mutable.StringBuilder.newBuilder
      insert.append("BEGIN BATCH ")
      var batchCount: Int = 0
      var total: Int = 0

      val statement: Statement = new SimpleStatement("select * from " + i.mappedTableName)
      statement.setFetchSize(fetchLimit)

      var resultSet : ResultSet = session.execute(statement)
      var iterator = resultSet.iterator()

      var defaultInsertStatement : String = buildDefaultInsertStatement(i.tableName, i.columns)

      while(iterator.hasNext) {
        var row: Row = iterator.next()
        insert.append(defaultInsertStatement)
        insert.append(i.findValuesOfColumns(row, session))

        batchCount += 1
        if(batchCount == batchLimit) { //against batch statement too large error
          batchCount = 0
          insert.append(" APPLY BATCH");
          session.execute(insert.toString())
          //println(total += batchCount)
          insert.clear
          insert.append("BEGIN BATCH ")
        }
      }
      //run the batch statement
      insert.append(" APPLY BATCH;")
      //println(insert.toString())
      session.execute(insert.toString())
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

}