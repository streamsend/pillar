package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{Row, Session}

import scala.collection.mutable

/**
  * Created by mustafa on 31.08.2016.
  */
class EagerFetch extends FetchType {
  var eagerMap: mutable.Map[Seq[AnyRef], AnyRef] = mutable.Map[Seq[AnyRef], AnyRef]()
  var keys: mutable.MutableList[String] = new mutable.MutableList[String]()

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    var localKeys: mutable.MutableList[String] = new mutable.MutableList[String]()

    keys.foreach((key: String) => {
      if(key.contains("$")) {
        val pattern = "'?\\$[a-zA-Z0-9_]*'?".r
        var parameter = pattern.findFirstIn(key).get.toString.trim
        if(parameter.contains("'"))
          parameter = parameter.substring(2, parameter.length-1)
        else
          parameter = parameter.substring(1)
        localKeys += row.getObject(parameter).toString.trim
      }
      else
        localKeys += key
    })

    val result: AnyRef = eagerMap(localKeys)
    result
  }
}