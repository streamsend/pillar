package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core._

import scala.collection.mutable

/**
  * Created by mgunes on 12.08.2016.
  */
class CqlStrategy (val mappedTableName: String) extends ModifyStrategy {
  var fetchType: FetchType = new LazyFetch(mappedTableName)

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    val result: AnyRef = fetchType.modify(columnProperty, row, session).asInstanceOf[AnyRef]
    result
  }

}

trait FetchType {
  def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef
}

class LazyFetch(val mappedTableName: String) extends FetchType {
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

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef= {
    var query: String = columnProperty.valueSource
    val valueList: mutable.MutableList[AnyRef] = new mutable.MutableList[AnyRef]
    val valueName: mutable.MutableList[String] = new mutable.MutableList[String]
    val valueClassName: mutable.MutableList[String] = new mutable.MutableList[String]
    var objName: String = ""

    if (query.contains("$")) {
      val pattern = "((( )+(in))|([a-zA-Z0-9_]+( )+=))( )+'?\\$[a-zA-Z0-9_]+'?".r

      for (m <- pattern.findAllIn(query)) {
        if(m.contains("in")) {
          var objName: String = "\\$[a-z]*".r.findFirstIn(m.toString).get
          objName = objName.substring(1) //delete $ sign
          var resultSet = session.execute("select " + objName + "from " + mappedTableName)
          valueName += objName

          var resultList: mutable.MutableList[String] = new mutable.MutableList[String]()
          while(resultSet.iterator().hasNext) {
            resultList += resultSet.iterator().next().getObject(objName).toString
          }
          var realValue =  resultList
          valueList += realValue
          valueClassName += columnProperty.dataType
          query = "'?\\$[a-z]*'?".r.replaceFirstIn(query, "?")
        }
        else if(m.contains("=") && m.contains("'") == true) {
          val arr: Array[String] = m.split("=")
          objName = arr(1).trim.substring(2, arr(1).trim.size-1)//'$obj', leave from ' and $
          var realValue: AnyRef = row.getObject(objName)
          valueList += realValue
          valueClassName += realValue.getClass.getName
          valueName += arr(0).trim
          query = "'\\$[a-zA-Z0-9_]+'".r.replaceFirstIn(query, "?")
        }
        else if(m.contains("=")){
          val arr: Array[String] = m.split("=")
          objName = arr(1).trim.substring(1, arr(1).trim.size)
          var realValue: AnyRef = row.getObject(objName)
          valueList += realValue
          query = "\\$[a-zA-Z0-9_]+".r.replaceFirstIn(query, "?")
          valueName += arr(0).trim
          valueClassName += realValue.getClass.getName
        }
      }
    }

    val preparedStatement: PreparedStatement = session.prepare(query) //to-do: add a log about invalid query
    var boundStatement: BoundStatement = new BoundStatement(preparedStatement)

    for(i<-0 until valueList.size) {
      valueClassName(i) match {
        case "java.math.BigDecimal" => boundStatement.setDecimal(valueName(i), valueList(i).asInstanceOf[java.math.BigDecimal])
        case "java.lang.Float" => boundStatement.setFloat(valueName(i), valueList(i).asInstanceOf[java.lang.Float])
        case "java.lang.Double" =>boundStatement.setDouble(valueName(i), valueList(i).asInstanceOf[java.lang.Double])
        case "java.math.BigIntege" => boundStatement.setVarint(valueName(i), valueList(i).asInstanceOf[java.math.BigInteger])
        case "java.util.Date" => boundStatement.setTimestamp(valueName(i), valueList(i).asInstanceOf[java.util.Date])
        case "java.util.UUID" => boundStatement.setUUID(valueName(i), valueList(i).asInstanceOf[java.util.UUID])
        case "java.lang.Long]" => boundStatement.setLong(valueName(i), valueList(i).asInstanceOf[java.lang.Long])
        case "java.lang.Integer" => boundStatement.setInt(valueName(i), valueList(i).asInstanceOf[java.lang.Integer])
        case "java.lang.String" => boundStatement.setString(valueName(i), valueList(i).asInstanceOf[java.lang.String])
        case "java.lang.Boolean" => boundStatement.setBool(valueName(i), valueList(i).asInstanceOf[java.lang.Boolean])
      }
    }

    boundStatement.bind()
    val result: AnyRef = session.execute(boundStatement).one().getObject(0)
    result
  }

}

class EagerFetch extends FetchType {
  var eagerMap: mutable.Map[Seq[String], AnyRef] = mutable.Map[Seq[String], AnyRef]()
  var keys: mutable.MutableList[String] = new mutable.MutableList[String]()

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    var localKeys: mutable.MutableList[String] = new mutable.MutableList[String]()

    for(i<-0 until keys.size) {
      if(keys(i).contains("$")) {
        val pattern = "'?\\$[a-z]*'?".r
        var parameter = pattern.findFirstIn(keys(i)).get.toString.trim
        if(parameter.contains("'"))
          parameter = parameter.substring(2, parameter.size-1)
        else
          parameter = parameter.substring(1)
        localKeys += row.getObject(parameter).toString.trim
      }
      else
        localKeys += keys(i)
    }
    val result: AnyRef = eagerMap(localKeys)
    result
  }

}
