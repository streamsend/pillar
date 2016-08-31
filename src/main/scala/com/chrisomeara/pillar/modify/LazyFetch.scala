package com.chrisomeara.pillar.modify

import java.util

import com.chrisomeara.pillar.{ColumnProperty, TypeBinding}
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Row, Session}

import scala.collection.mutable

/**
  * Created by mustafa on 30.08.2016.
  */
class LazyFetch(val mappedTableName: String) extends FetchType {

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef= {
    var query: String = columnProperty.valueSource
    val bindRowList: mutable.MutableList[BindRow] = new mutable.MutableList[BindRow]

    if (query.contains("$")) {
      val pattern = "((( )+(in))|([a-zA-Z0-9_]+( )+=))( )+'?\\$[a-zA-Z0-9_]+'?".r

      for (matched <- pattern.findAllIn(query)) {
        if(matched.contains("in")) {
          query = "'?\\$[a-z]*'?".r.replaceFirstIn(query, "?")
          bindRowList += bindRowForIn(session, matched)
        }
        else if(matched.contains("=") && matched.contains("'")) {
          query = "'\\$[a-zA-Z0-9_]+'".r.replaceFirstIn(query, "?")
          bindRowList += bindRowApostrophe(row, matched)
        }
        else if(matched.contains("=")){
          query = "\\$[a-zA-Z0-9_]+".r.replaceFirstIn(query, "?")
          bindRowList += bindRowNonApostrophe(row, matched)
        }
      }
    }

    val preparedStatement: PreparedStatement = session.prepare(query) //to-do: add a log about invalid query
    var boundStatement: BoundStatement = new BoundStatement(preparedStatement)

    for(i <- bindRowList.indices) {
      boundStatement = TypeBinding.setBoundStatementJavaTypes(
        boundStatement, bindRowList(i).dataType, bindRowList(i).value, i)
    }

    boundStatement.bind()
    val result: AnyRef = session.execute(boundStatement).one().getObject(0)
    result
  }

  def bindRowForIn(session: Session, matched: String): BindRow = {
    var objName: String = "\\$[a-z]*".r.findFirstIn(matched.toString).get
    objName = objName.substring(1) //delete $ sign
    val resultSet = session.execute("select " + objName + " from " + mappedTableName)
    val resultList: java.util.List[AnyRef] = new util.ArrayList[AnyRef]()

    while(resultSet.iterator().hasNext) {
      resultList.add(resultSet.iterator().next().getObject(objName))
    }

    BindRow(objName, resultList.getClass.getName, resultList)
  }

  def bindRowApostrophe(row: Row, matched: String): BindRow = {
    val arr: Array[String] = matched.split("=")
    val objName = arr(1).trim.substring(2, arr(1).trim.length-1)//'$obj', leave from ' and $
    val realValue: AnyRef = row.getObject(objName)
    BindRow(arr(0).trim, realValue.getClass.getName, realValue)
  }

  def bindRowNonApostrophe(row: Row, matched: String): BindRow = {
    val arr: Array[String] = matched.split("=")
    val objName = arr(1).trim.substring(1, arr(1).trim.length)
    val realValue: AnyRef = row.getObject(objName)
    BindRow(arr(0).trim, realValue.getClass.getName, realValue)
  }
}
