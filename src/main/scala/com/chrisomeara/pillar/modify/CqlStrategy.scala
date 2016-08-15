package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{Row, Session}

import scala.collection.mutable

/**
  * Created by mgunes on 12.08.2016.
  */
class CqlStrategy extends ModifyStrategy{
  var fetchType: FetchType = new LazyFetch

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): String = {
    val result: String = fetchType.modify(columnProperty, row, session)
    result
  }

}

trait FetchType {
  def modify(columnProperty: ColumnProperty, row: Row, session: Session): String
}

class LazyFetch extends FetchType {
  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): String = {
    var query: String = columnProperty.valueSource
    if (query.contains("$")) {
      val pattern = "\\$[a-z]*".r

      for (m <- pattern.findAllIn(query)) {
        //replace variables with their real value
        val realValue = row.getObject(m.substring(1, m.size))
        query = pattern.replaceFirstIn(query, realValue.toString)
      }
    }
    session.execute(query).one().getObject(columnProperty.name).toString.trim
  }
}

class EagerFetch extends FetchType {
  var eagerMap: mutable.Map[Seq[String], String] = mutable.Map[Seq[String], String]()
  var keys: mutable.MutableList[String] = new mutable.MutableList[String]()

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): String = {
    var localKeys: mutable.MutableList[String] = new mutable.MutableList[String]()

    for(i<-0 until keys.size) {
      if(keys(i).contains("$")) {
        val pattern = "('\\$|')"
        var parameter: Array[String] = keys(0).split(pattern)
        localKeys += row.getObject(parameter(1)).toString.trim
      }
      else
        localKeys += keys(i)
    }

    val result: String = eagerMap.get(localKeys).get.trim
    result
  }

}
