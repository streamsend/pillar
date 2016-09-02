package com.chrisomeara.pillar

import com.chrisomeara.pillar.modify.BindRow
import com.datastax.driver.core._

import scala.collection.mutable

/**
  * Created by mgunes on 05.08.2016.
  */
class MigrateeTable {
  var tableName : String = _
  var mappedTableName : String = _
  var mappedTableColumns = new mutable.MutableList[String]
  var columns: mutable.Map[String, ColumnProperty] = scala.collection.mutable.Map[String, ColumnProperty]()
  var primaryKeyColumns = new mutable.MutableList[String]
  var cassandraStringDataTypes = List("text", "ascii", "varchar", "inet", "timestamp")

  def readColumnsMetadata(session: Session): Unit = {
    val tableMetadata = session.getCluster.getMetadata.getKeyspace(session.getLoggedKeyspace).getTable(tableName)
    readColumnNamesAndTypes(tableMetadata)
    readPrimaryKeys(tableMetadata)
    readMappedTableColumnNames(session)
  }

  def readMappedTableColumnNames(session: Session): Unit = {
    val iterator = session.getCluster.getMetadata.getKeyspace(session.getLoggedKeyspace).getTable(mappedTableName).getColumns.listIterator()

    while(iterator.hasNext) {
      mappedTableColumns += iterator.next().getName
    }
  }

  def readPrimaryKeys(tableMetadata: TableMetadata): Unit = {
    val pkIterator = tableMetadata.getPrimaryKey.listIterator()

    while(pkIterator.hasNext) {
      primaryKeyColumns += pkIterator.next.getName
    }
  }

  def readColumnNamesAndTypes(tableMetadata: TableMetadata): Unit = {
    val iterator = tableMetadata.getColumns.iterator()
    while(iterator.hasNext) {
      val column:ColumnMetadata = iterator.next()
      val columnName = column.getName

      if(columns.contains(columnName)) {
          columns(columnName).dataType = column.getType.toString
          columns(columnName).columnClass = TypeBinding.findClass(columns(columnName).dataType)
      }
      else {
        val columnProperty = new ColumnProperty(columnName)
        columnProperty.dataType = column.getType.toString
        columnProperty.columnClass = TypeBinding.findClass(columnProperty.dataType)
        columns += (columnName -> columnProperty)
      }
    }
  }

  def findValuesOfColumns(row: Row, session: Session): BoundStatement = {
    val bindRowList: mutable.MutableList[BindRow] = buildBindRowList(session, row)
    val insertStatement: String = buildInsertStatement()

    val preparedStatement: PreparedStatement = session.prepare(insertStatement)
    var boundStatement: BoundStatement = new BoundStatement(preparedStatement)

    for(p <- bindRowList.indices) {
      boundStatement = TypeBinding.setBoundStatementCassandraTypes(
        boundStatement, bindRowList(p).dataType, bindRowList(p).value, bindRowList(p).columnName)
    }

    boundStatement.bind()
  }

  def buildInsertStatement(): String = {
    val insertStatement = StringBuilder.newBuilder
    insertStatement.append("INSERT INTO " + tableName + " (")
    columns.keySet.foreach((key: String) => insertStatement.append(key + ","))
    insertStatement.deleteCharAt(insertStatement.size-1) //delete last comma
    insertStatement.append(") VALUES (")

    for(p <- 0 until columns.keySet.size)
      insertStatement.append("?,")

    insertStatement.deleteCharAt(insertStatement.size-1) //delete last comma
    insertStatement.append(");")

    insertStatement.toString
  }

  def buildBindRowList(session: Session, row: Row): mutable.MutableList[BindRow] = {
    val bindRowList: mutable.MutableList[BindRow] = new mutable.MutableList[BindRow]

    columns.keySet.foreach(f = (key: String) => {
      try {
        val result: AnyRef = columns(key).modifyOperation.modify(columns(key), row, session)
        bindRowList += BindRow(columns(key).name, columns(key).dataType, result)
      } catch {
        case e: Exception =>
      }
    })
    bindRowList
  }

  def primaryKeyNullControl(): Boolean = {
    var control: Boolean = true
    val iterator = primaryKeyColumns.iterator

    while(control && iterator.hasNext) {
      val column = iterator.next()
      if(columns(column).valueSource.equalsIgnoreCase("no-source") && !mappedTableColumns.contains(column))
        control = false
    }
    control
  }
}
