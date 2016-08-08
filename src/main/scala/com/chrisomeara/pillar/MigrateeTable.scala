package com.chrisomeara.pillar

import scala.collection.mutable

/**
  * Created by mgunes on 05.08.2016.
  */
class MigrateeTable {
  var tableName : String = _
  var mappedTableName : String = _
  var tableColumnList = new mutable.MutableList[String]()
  var mappedTableColumnList  =  new mutable.MutableList[String]()
  var columnValueSource : mutable.Map[String, String] =  scala.collection.mutable.Map[String, String]()
}
