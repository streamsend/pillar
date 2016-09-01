package com.chrisomeara.pillar

import com.datastax.driver.core.BoundStatement

/**
  * Created by mustafa on 30.08.2016.
  */
object TypeBinding {
  def setBoundStatementCassandraTypes(boundStatement: BoundStatement, dataType: String, value: AnyRef, columnName: String): BoundStatement = {
    dataType match {
      case "decimal" => boundStatement.setDecimal(columnName, value.asInstanceOf[java.math.BigDecimal])
      case "float" => boundStatement.setFloat(columnName, value.asInstanceOf[java.lang.Float])
      case "double" =>boundStatement.setDouble(columnName, value.asInstanceOf[java.lang.Double])
      case "varint" => boundStatement.setVarint(columnName, value.asInstanceOf[java.math.BigInteger])
      case "timestamp" => boundStatement.setTimestamp(columnName, value.asInstanceOf[java.util.Date])
      case "timeuuid" => boundStatement.setUUID(columnName, value.asInstanceOf[java.util.UUID])
      case "uuid" => boundStatement.setUUID(columnName, value.asInstanceOf[java.util.UUID])
      case "bigint" => boundStatement.setLong(columnName, value.asInstanceOf[java.lang.Long])
      case "int" => boundStatement.setInt(columnName, value.asInstanceOf[java.lang.Integer])
      case "varchar" => boundStatement.setString(columnName, value.asInstanceOf[java.lang.String])
      case "text" => boundStatement.setString(columnName, value.asInstanceOf[java.lang.String])
      case "boolean" => boundStatement.setBool(columnName, value.asInstanceOf[java.lang.Boolean])
      case _ =>
        if(dataType.contains("list"))
          boundStatement.setList(columnName, value.asInstanceOf[java.util.List[AnyRef]])
        else if(dataType.contains("map"))
          boundStatement.setMap(columnName, value.asInstanceOf[java.util.Map[AnyRef, AnyRef]])
        else if(dataType.contains("set"))
          boundStatement.setSet(columnName, value.asInstanceOf[java.util.Set[AnyRef]])
    }
    boundStatement
  }

  def setBoundStatementJavaTypes(boundStatement: BoundStatement, dataType: String, value: AnyRef, column: Int): BoundStatement = {
    dataType match {
      case "java.math.BigDecimal" => boundStatement.setDecimal(column, value.asInstanceOf[java.math.BigDecimal])
      case "java.lang.Float" => boundStatement.setFloat(column, value.asInstanceOf[java.lang.Float])
      case "java.lang.Double" =>boundStatement.setDouble(column, value.asInstanceOf[java.lang.Double])
      case "java.math.BigInteger" => boundStatement.setVarint(column, value.asInstanceOf[java.math.BigInteger])
      case "java.util.Date" => boundStatement.setTimestamp(column, value.asInstanceOf[java.util.Date])
      case "java.util.UUID" => boundStatement.setUUID(column, value.asInstanceOf[java.util.UUID])
      case "java.lang.Long" => boundStatement.setLong(column, value.asInstanceOf[java.lang.Long])
      case "java.lang.Integer" => boundStatement.setInt(column, value.asInstanceOf[java.lang.Integer])
      case "java.lang.String" => boundStatement.setString(column, value.asInstanceOf[java.lang.String])
      case "java.lang.Boolean" => boundStatement.setBool(column, value.asInstanceOf[java.lang.Boolean])
      case "java.util.ArrayList" => boundStatement.setList(column, value.asInstanceOf[java.util.List[AnyRef]])
      case "java.util.Map" => boundStatement.setMap(column, value.asInstanceOf[java.util.HashMap[AnyRef, AnyRef]])
      case "java.util.Set" => boundStatement.setSet(column, value.asInstanceOf[java.util.HashSet[AnyRef]])
    }
    boundStatement
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
      case "uuid" => fClass = classOf[java.util.UUID]
      case "bigint" => fClass = classOf[java.lang.Long]
      case "text" => fClass = classOf[java.lang.String]
      case "varchar" => fClass = classOf[java.lang.String]
      case "int" => fClass = classOf[java.lang.Integer]
      case "boolean" => fClass = classOf[java.lang.Boolean]
      case _ =>
        if(dataType.contains("list"))
          fClass = classOf[java.util.List[AnyRef]]
        else if(dataType.contains("map"))
          fClass = classOf[java.util.Map[AnyRef, AnyRef]]
        else if(dataType.contains("set"))
        fClass = classOf[java.util.Set[AnyRef]]
    }
    fClass
  }
}
