package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Row, Session}
import scala.collection.mutable

/**
  * Created by mgunes on 12.08.2016.
  */
class CqlStrategy (val mappedTableName: String) extends ModifyStrategy {
  var fetchType: FetchType = new LazyFetch(mappedTableName)

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): String = {
    val result: String = fetchType.modify(columnProperty, row, session)
    result
  }

}

trait FetchType {
  def modify(columnProperty: ColumnProperty, row: Row, session: Session): String
}

class LazyFetch(val mappedTableName: String) extends FetchType {

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): String = {
    var query: String = columnProperty.valueSource
    val valueList: mutable.MutableList[AnyRef] = new mutable.MutableList[AnyRef]
    val valueName: mutable.MutableList[String] = new mutable.MutableList[String]
    val valueClassName: mutable.MutableList[String] = new mutable.MutableList[String]
    var objName: String = ""

    if (query.contains("$")) {
      val pattern = "(( )*(in)( )*)?'?\\$[a-z]*'?".r

      for (m <- pattern.findAllIn(query)) {

        if(m.contains("in")) {
          var objName: String = "\\$[a-z]*".r.findFirstIn(m.toString).get
          objName = objName.substring(1) //delete $ sign
          var resultSet = session.execute("select " + objName + "from " + mappedTableName)

          var resultList: mutable.MutableList[String] = new mutable.MutableList[String]()
          while(resultSet.iterator().hasNext) {
            resultList += resultSet.iterator().next().getObject(objName).toString
          }
          var realValue =  resultList
          valueList += realValue
          valueClassName += realValue.getClass.getName
          query = "'?\\$[a-z]*'?".r.replaceFirstIn(query, "?")
        }
        else if(m.contains("'") == true) {
          objName = m.substring(2, m.size-1)//'$obj', leave from ' and $
          var realValue: AnyRef = row.getObject(objName)
          valueList += realValue
          valueClassName += realValue.getClass.getName
          query = pattern.replaceFirstIn(query, "?")
        }
        else {
          objName = m.substring(1, m.size)
          var realValue: AnyRef = row.getObject(objName)
          valueList += realValue
          query = pattern.replaceFirstIn(query, "?")
          valueClassName += realValue.getClass.getName
        }

        valueName += objName
        //valueClass += realValue.getClass

      }
    }

    val arrValues: Array[AnyRef] = new Array[AnyRef](valueList.size)

    val preparedStatement: PreparedStatement = session.prepare(query) //to-do: add a log about invalid query
    val boundStatement: BoundStatement = new BoundStatement(preparedStatement)

    for(i<-0 until valueList.size) {
      //arrValues(i) = valueList(i)
      valueClassName(i) match {
        case "java.lang.Integer" => boundStatement.setInt(valueName(i), Integer.parseInt(valueList(i).toString))
        case "java.lang.String" => boundStatement.setString(valueName(i), valueList(i).toString)
        case _ => println("Unknown data type")
      }
    }

    val result: String = session.execute(boundStatement).one().get(columnProperty.name, columnProperty.columnClass).toString.trim
    result
  }
}

class EagerFetch extends FetchType {
  var eagerMap: mutable.Map[Seq[String], String] = mutable.Map[Seq[String], String]()
  var keys: mutable.MutableList[String] = new mutable.MutableList[String]()

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): String = {
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
    val result: String = eagerMap.get(localKeys).get.trim
    result
  }

}
