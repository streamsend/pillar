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
  var tableColumnList = new mutable.MutableList[String]()
  //var mappedTableColumnList  =  new mutable.MutableList[String]() is it necessary?
  var columnValueSource : mutable.Map[String, String] =  scala.collection.mutable.Map[String, String]()

  def readColumnNames(session: Session): Unit = {
    val s = session.execute("select column_name " +
      "from system.schema_columns " +
      "where keyspace_name ='" + session.getLoggedKeyspace + "' and columnfamily_name = '" + tableName + "'")

    val iterator = s.iterator()
    while(iterator.hasNext) {
      var columnName : Array[String] = iterator.next.toString.split("Row\\[|\\]")
      tableColumnList += columnName(1)
    }
  }

  def newValueFromShFile(columnName: String, row: Row): String = {
    var resource: String = columnValueSource.get(columnName).get
    val arr: Array[String] = resource.split(" ")
    var processSh: String = "sh " + arr(0) //add path

    for (j <- 1 to arr.size - 1) {
      if (arr(j).contains("$")) {
        var parameter: Array[String] = arr(j).split("\\$") //variable parameter

        processSh += " " + row.getObject(parameter(1))
      }
      else
        processSh += " " + arr(j)
    }
    Process(processSh).!!.trim
  }

  def newValueFromSqlQuery(columnName: String, row: Row, session: Session): String = {
    var query: String = columnValueSource.get(columnName).get.toString
    if (query.contains("$")) {
      val pattern = "\\$[a-z]*".r

      for (m <- pattern.findAllIn(query)) {
        //replace variables with their real value
        val realValue = row.getObject(m.substring(1, m.size))
        query = pattern.replaceFirstIn(query, realValue.toString)
      }
    }
    session.execute(query).one().getObject(columnName).toString.trim
  }

  def findValuesOfColumns(row: Row, session: Session): String = {
    var result: Any = ""
    var valuesStatement: String = ""

    //find values each column
    tableColumnList.foreach((columnName: String) => {
      try {
        if (columnValueSource.contains(columnName)) //sh or sql
          if (columnValueSource.get(columnName).toString.contains(".sh")) //from sh file
            result = newValueFromShFile(columnName, row)
          else //from sql query
            result = newValueFromSqlQuery(columnName, row, session)
        else //default value
          result = row.getObject(columnName)

        val columnDataType: String = session.getCluster.getMetadata.getKeyspace(session.getLoggedKeyspace).getTable(tableName).getColumn(columnName).getType.toString
        if (columnDataType.contains("text") || columnDataType.contains("ascii") || columnDataType.contains("varchar"))
          valuesStatement += "'" + result + "',"
        else
          valuesStatement += result + ","
      } catch {
        case e : IllegalArgumentException => { //not found default column
          var result = "null"
          valuesStatement += result + ","
        }
      }
    })

    valuesStatement
  }
}
