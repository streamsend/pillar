package com.chrisomeara.pillar.modify

import com.chrisomeara.pillar.ColumnProperty
import com.datastax.driver.core._

/**
  * Created by mgunes on 12.08.2016.
  */
class CqlStrategy (val mappedTableName: String) extends ModifyStrategy {
  var fetchType: FetchType = new LazyFetch(mappedTableName)

  override def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef = {
    val result: AnyRef = fetchType.modify(columnProperty, row, session)
    result
  }

}

trait FetchType {
  def modify(columnProperty: ColumnProperty, row: Row, session: Session): AnyRef
}

case class BindRow(columnName: String, dataType: String, value: AnyRef)
