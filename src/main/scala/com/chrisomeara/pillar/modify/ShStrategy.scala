package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{Row, Session}

import scala.sys.process.Process

/**
  * Created by mgunes on 12.08.2016.
  */
class ShStrategy extends ModifyStrategy{

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    var resource: String = columnProperty.valueSource
    val arr: Array[String] = resource.split(" ")
    var processSh: String = "sh " + arr(0) //add path

    for (j <- 1 to arr.size - 1) {
      if (arr(j).contains("$")) {
        var parameter: Array[String] = arr(j).split("\\$") //variable parameter
        processSh += " " + row.get(parameter(1), columnProperty.columnClass)
      }
      else
        processSh += " " + arr(j)
    }
    val result: AnyRef = Process(processSh).!!.trim

    result
  }
}
