package com.chrisomeara.pillar

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
      var column:ColumnMetadata = iterator.next()
      var columnName = column.getName
      var dataType = column.getType.toString
      var columnClass = column.getType.getClass

      if(columns.contains(columnName)) {
        columns.get(columnName).get.dataType = dataType
        if(cassandraStringDataTypes.contains(dataType))
          columns.get(columnName).get.columnClass = classOf[String]
        else
          columns.get(columnName).get.columnClass = classOf[Integer]
      }
      else {
        var columnProperty = new ColumnProperty(columnName)
        columnProperty.dataType = dataType
        if(cassandraStringDataTypes.contains(dataType))
          columnProperty.columnClass = classOf[String]
        else
          columnProperty.columnClass = classOf[Int]
        columns += (columnName -> columnProperty)
      }
    }
  }

  def findValuesOfColumns(row: Row, session: Session):BoundStatement = {
    var result: AnyRef = null
    val valuesStatement: mutable.MutableList[AnyRef] = new mutable.MutableList[AnyRef]
    val columnName:  mutable.MutableList[String] = new mutable.MutableList[String]
    val columnValue:  mutable.MutableList[AnyRef] = new mutable.MutableList[AnyRef]
    val columnClass:  mutable.MutableList[Class[_<:Any]] = new mutable.MutableList[Class[_<:Any]]

    var i = 0
    var dis: String = "INSERT INTO " + tableName + " ("

    columns.keySet.foreach((key: String) => {
      try {
        result = columns(key).modifyOperation.modify(columns(key), row, session)
        valuesStatement += result
        dis += key + ","
        i = i +1

        columnName += columns(key).name
        columnValue += result
        columnClass += columns(key).columnClass
      } catch {
        case e : Exception => {
          var result = "null"
          e.printStackTrace()
          //valuesStatement(i) = null
        }
      }
    })

    val valuesStatement2: Array[AnyRef] = valuesStatement.toArray
    dis = dis.substring(0, dis.size - 1) //delete last comma
    dis += ") VALUES ("

    for(p<-0 until i) {dis += "?,"}
    dis = dis.substring(0, dis.size - 1) //delete last comma
    dis += ");"

    val preparedStatement: PreparedStatement = session.prepare(dis)
    val boundStatement: BoundStatement = new BoundStatement(preparedStatement)

    /*for(p<-0 until i) {
      boundStatement.set(columnName(p), columnValue(p), columnClass(p))
    }*/

    for(p<-0 until i) {
      if(columnClass(p).getName.contains("Integer")) {
        var xx: Int = Integer.parseInt(columnValue(p).toString)
        boundStatement.setInt(columnName(p), xx)
      }
      else
        boundStatement.setString(columnName(p), columnValue(p).asInstanceOf[String])
    }

   // boundStatement.set("name", "14", classOf[String])
    //boundStatement.set("name", 14, classOf[Int])
    boundStatement.bind()
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
