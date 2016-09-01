package com.chrisomeara.pillar

import java.util.Date

import com.chrisomeara.pillar.modify.CqlStrategy
import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by mgunes on 16.08.2016.
  */
class IrreversibleModifiableMigration(val description: String, val authoredAt: Date, val fetch: String, val up: Seq[String], val mapping1: Seq[MigrateeTable]) extends Migration {
  val mapping: Seq[MigrateeTable] = mapping1
  private val batchStatement: BatchStatement = new BatchStatement()

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

  def isContainsBiggerLessThanOperator: Boolean = {
    mapping.foreach((migrateeTable: MigrateeTable) => {
      migrateeTable.columns.keySet.foreach((key: String) => {
        if(migrateeTable.columns(key).modifyOperation.isInstanceOf[CqlStrategy])
          if(migrateeTable.columns(key).valueSource.contains("<") || migrateeTable.columns(key).valueSource.contains(">"))
            return false
       })
    })
    true
  }

  override def executeTableStatement(session: Session): Unit = {
    val config = ConfigFactory.load()
    val batchLimit: Int = config.getInt("cassandra-batch-size")
    val fetchLimit: Int = config.getInt("cassandra-fetch-size")

    mapping.foreach((migrateeTable: MigrateeTable) => migrateeTable.readColumnsMetadata(session))

    if(fetch.equalsIgnoreCase("eager")) {
      if(!isContainsBiggerLessThanOperator)
        println("Eager converted to lazy becaues of < or > operators")
      else
        eager(session, fetchLimit)
    }

    primaryKeyNullControl()

    //create batch statements for each table
    for(mappingTable <- mapping) {
      //create batch statement
      var batchCount: Int = 0
      val statement: Statement = new SimpleStatement("select * from " + mappingTable.mappedTableName)
      statement.setFetchSize(fetchLimit)

      val resultSet : ResultSet = session.execute(statement)
      val iterator = resultSet.iterator()

      while(iterator.hasNext) {
        val row: Row = iterator.next()
        batchStatement.add(mappingTable.findValuesOfColumns(row, session))

        batchCount += 1
        if(batchCount == batchLimit) { //against batch statement too large error
          session.execute(batchStatement)
          batchStatement.clear()
          batchCount = 0
        }
      }
      //run the batch statement
      session.execute(batchStatement)
      println("Migrations in the file has finished for " + mappingTable.tableName)
    }
  }

  def buildDefaultInsertStatement(tableName: String, columns: mutable.Map[String, ColumnProperty]): String = {
    var dis: String = "INSERT INTO " + tableName + " ("

    columns.keySet.foreach((key: String) =>  dis += key + ",")
    dis = dis.substring(0, dis.length - 1) //delete last comma
    dis += ") VALUES ("

    columns.keySet.foreach(_ => dis += "?,")
    dis = dis.substring(0, dis.length - 1) //delete last comma
    dis += ");"

    dis
  }

}