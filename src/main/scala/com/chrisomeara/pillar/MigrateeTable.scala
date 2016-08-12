package com.chrisomeara.pillar

import com.datastax.driver.core.{ColumnMetadata, Row, Session, TableMetadata}

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
  var cassandraDataTypes = List("text", "ascii", "varchar", "inet", "timestamp")

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
      var column:ColumnMetadata = iterator.next()
      var columnName = column.getName
      var dataType = column.getType.toString

      if(columns.contains(columnName)) {
        columns.get(columnName).get.dataType = dataType
      }
      else {
        var columnProperty = new ColumnProperty(columnName)
        columnProperty.dataType = dataType
        columns += (columnName -> columnProperty)
      }
    }
  }

  def findValuesOfColumns(row: Row, session: Session): String = {
    var result: Any = ""
    var valuesStatement: String = ""

    columns.keySet.foreach((key: String) => {
      try {
        result = columns.get(key).get.modifyOperation.modify(columns.get(key).get, row, session)

        if (cassandraDataTypes.contains(columns.get(key).get.dataType))
          valuesStatement += "'" + result + "',"
        else
          valuesStatement += result + ","
      } catch {
        case e : Exception => {
          var result = "null"
          valuesStatement += result + ","
        }
      }
    })
    valuesStatement
  }

  def primaryKeyNullControl(): Boolean = {
    var control: Boolean = true
    val iterator = primaryKeyColumns.iterator

    while(control == true && iterator.hasNext) {
      var column = iterator.next()
      if(columns.get(column).get.valueSource.equalsIgnoreCase("no-source") && !mappedTableColumns.contains(column))
        control = false
    }
    control
  }
}
