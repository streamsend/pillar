package com.chrisomeara.pillar

import java.util

import scala.collection.mutable

/**
  * Created by mgunes on 05.08.2016.
  */
class MigrateeTable {
  var tableName : String = _
  var defaultTableName : String = _
  var tableColumnList : List[String] = _
  var defaultTableColumnList :List[String] = _
  var columnValueSource : Map[String, String] = _
}
