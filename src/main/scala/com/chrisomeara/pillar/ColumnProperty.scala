package com.chrisomeara.pillar

import com.chrisomeara.pillar.modify.{ModifyStrategy, NoModify}

/**
  * Created by mgunes on 12.08.2016.
  */
class ColumnProperty(columnName: String) {

  var name: String = columnName
  var dataType: String = _
  var valueSource: String =  "no-source"
  var modifyOperation: ModifyStrategy = new NoModify

}
