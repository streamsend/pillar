package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core.{Row, Session}

/**
  * Created by mgunes on 12.08.2016.
  */
class NoModify extends ModifyStrategy{

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    var defaultValue: AnyRef = null

    if(row.getColumnDefinitions.getType(columnProperty.name).isCollection)
       defaultValue = row.getObject(columnProperty.name)
    else
      defaultValue = row.get(columnProperty.name, columnProperty.columnClass).asInstanceOf[AnyRef]

    defaultValue
  }
}
