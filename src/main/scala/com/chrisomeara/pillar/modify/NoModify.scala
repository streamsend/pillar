package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{Row, Session}

/**
  * Created by mgunes on 12.08.2016.
  */
class NoModify extends ModifyStrategy{

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    val defaultValue = row.get(columnProperty.name, columnProperty.columnClass).asInstanceOf[AnyRef]
    defaultValue
  }
}
