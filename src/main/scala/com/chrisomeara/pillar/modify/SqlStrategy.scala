package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{Row, Session}

/**
  * Created by mgunes on 12.08.2016.
  */
class SqlStrategy extends ModifyStrategy{

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
