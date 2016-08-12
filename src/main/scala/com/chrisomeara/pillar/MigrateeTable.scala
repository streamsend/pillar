package com.chrisomeara.pillar

import com.datastax.driver.core.{Row, Session}

import scala.collection.mutable
import scala.sys.process.Process

/**
  * Created by mgunes on 05.08.2016.
  */
class MigrateeTable {
  var tableName : String = _
  var mappedTableName : String = _
  var columns: mutable.Map[String, ColumnProperty] =scala.collection.mutable.Map[String, ColumnProperty]()
  var cassandraDataTypes: List[String] = List("text", "ascii", "varchar", "inet", "timestamp")

  def readColumnNamesAndTypes(session: Session): Unit = {
    val s = session.execute("select column_name " +
      "from system.schema_columns " +
      "where keyspace_name ='" + session.getLoggedKeyspace + "' and columnfamily_name = '" + tableName + "'")

    val iterator = s.iterator()
    while(iterator.hasNext) {
      var columnName : Array[String] = iterator.next.toString.split("Row\\[|\\]")
      var dataType: String = session.getCluster.getMetadata.getKeyspace(session.getLoggedKeyspace).getTable(tableName).getColumn(columnName(1)).getType.toString
      if(columns.contains(columnName(1))) {
        columns.get(columnName(1)).get.dataType = dataType
      }
      else {
        var columnProperty = new ColumnProperty(columnName(1))
        columnProperty.dataType = dataType
        columns += (columnName(1) -> columnProperty)
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
}
