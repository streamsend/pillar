package com.chrisomeara.pillar

import com.datastax.driver.core.BoundStatement

/**
  * Created by mustafa on 30.08.2016.
  */
object TypeBinding {
  def setBoundStatement(boundStatement: BoundStatement, dataType: String, value: AnyRef, column: Int): BoundStatement = {
    dataType match {
      case "decimal" => boundStatement.setDecimal(column, value.asInstanceOf[java.math.BigDecimal])
      case "float" => boundStatement.setFloat(column, value.asInstanceOf[java.lang.Float])
      case "double" =>boundStatement.setDouble(column, value.asInstanceOf[java.lang.Double])
      case "varint" => boundStatement.setVarint(column, value.asInstanceOf[java.math.BigInteger])
      case "timestamp" => boundStatement.setTimestamp(column, value.asInstanceOf[java.util.Date])
      case "timeuuid" => boundStatement.setUUID(column, value.asInstanceOf[java.util.UUID])
      case "uuid" => boundStatement.setUUID(column, value.asInstanceOf[java.util.UUID])
      case "bigint" => boundStatement.setLong(column, value.asInstanceOf[java.lang.Long])
      case "int" => boundStatement.setInt(column, value.asInstanceOf[java.lang.Integer])
      case "varchar" => boundStatement.setString(column, value.asInstanceOf[java.lang.String])
      case "text" => boundStatement.setString(column, value.asInstanceOf[java.lang.String])
      case "boolean" => boundStatement.setBool(column, value.asInstanceOf[java.lang.Boolean])
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
    }
    fClass
  }
}
