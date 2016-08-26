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
      var columnClass = column.getClass

      if(columns.contains(columnName)) {
          columns(columnName).dataType = dataType
          columns(columnName).columnClass = findClass(dataType) //getClasss dene
      }
      else {
        var columnProperty = new ColumnProperty(columnName)
        columnProperty.dataType = dataType
        columnProperty.columnClass = findClass(dataType)
        columns += (columnName -> columnProperty)
      }
    }
  }

  def findClass(dataType: String): Class[_] = {
    var fClass: Class[_] = null

    dataType match {
      case "decimal" => fClass = classOf[java.math.BigDecimal]
      case "float" => fClass = classOf[java.lang.Float]
      case "double" => fClass = classOf[java.lang.Double]
      case "varint" => fClass = classOf[java.math.BigInteger]
      case "timestamp" => fClass = classOf[java.util.Date]
      case "timeuuid" => fClass = classOf[java.util.UUID]
      case "bigint" => fClass = classOf[java.lang.Long]
      case "text" => fClass = classOf[java.lang.String]
      case "varchar" => fClass = classOf[java.lang.String]
      case "int" => fClass = classOf[java.lang.Integer]
    }

    fClass
  }

  def findValuesOfColumns(row: Row, session: Session): BoundStatement = {
    var result: AnyRef = null
    val valuesStatement: mutable.MutableList[AnyRef] = new mutable.MutableList[AnyRef]
    val columnName:  mutable.MutableList[String] = new mutable.MutableList[String]
    val columnValue:  mutable.MutableList[AnyRef] = new mutable.MutableList[AnyRef]
    val columnClassName:  mutable.MutableList[String] = new mutable.MutableList[String]

    var i = 0
    var dis: String = "INSERT INTO " + tableName + " ("

    columns.keySet.foreach((key: String) => {
      try {
        result = columns(key).modifyOperation.modify(columns(key), row, session)
        valuesStatement += result
        dis += key + ","
        i = i +1

        columnName += columns(key).name
        columnValue += result.asInstanceOf[AnyRef]
        columnClassName += columns(key).dataType
      } catch {
        case e : Exception => {
          var result = "null"
          e.printStackTrace()
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

    for(p<-0 until i) {
      columnClassName(p) match {
        case "decimal" => boundStatement.setDecimal(columnName(p), columnValue(p).asInstanceOf[java.math.BigDecimal])
        case "float" => boundStatement.setFloat(columnName(p), columnValue(p).asInstanceOf[java.lang.Float])
        case "double" =>boundStatement.setDouble(columnName(p), columnValue(p).asInstanceOf[java.lang.Double])
        case "varint" => boundStatement.setVarint(columnName(p), columnValue(p).asInstanceOf[java.math.BigInteger])
        case "timestamp" => boundStatement.setTimestamp(columnName(p), columnValue(p).asInstanceOf[java.util.Date])
        case "timeuuid" => boundStatement.setUUID(columnName(p), columnValue(p).asInstanceOf[java.util.UUID])
        case "bigint" => boundStatement.setLong(columnName(p), columnValue(p).asInstanceOf[java.lang.Long])
        case "int" => boundStatement.setInt(columnName(p), columnValue(p).asInstanceOf[java.lang.Integer])
        case "varchar" => boundStatement.setString(columnName(p), columnValue(p).asInstanceOf[java.lang.String])
        case "text" => boundStatement.setString(columnName(p), columnValue(p).asInstanceOf[java.lang.String])
        case "boolean" => boundStatement.setBool(columnName(p), columnValue(p).asInstanceOf[java.lang.String].asInstanceOf[java.lang.Boolean])
      }
    }

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
