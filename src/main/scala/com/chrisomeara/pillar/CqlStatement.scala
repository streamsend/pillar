package com.chrisomeara.pillar

import com.chrisomeara.pillar.modify.{CqlStrategy, EagerFetch}
import com.datastax.driver.core._

import scala.collection.mutable

/**
  * Created by mgunes on 15.08.2016.
  */
case class CqlStatement(val value: String, val tableName: String, val keys: Seq[String], val findKeys: mutable.MutableList[String])

object CqlStatement {
  def parseCqlStatement(query: String): CqlStatement = {
    //val arr: Array[String] = query.split("(select|from|where|and|or)")
    val pattern = "(( )*(in)( )*)?'?\\$[a-z]*'?".r
    var keys: mutable.MutableList[String] = new mutable.MutableList[String]()
    var findKeys: mutable.MutableList[String] = new mutable.MutableList[String]()

    /*for(i<-3 until arr.size) {
      var keysArr = arr(i).split("=")
      keys += keysArr(0).trim
      findKeys += keysArr(1).trim
    }*/
//    val cqlStatement: CqlStatement = new CqlStatement(arr(1).trim, arr(2).trim, keys, findKeys)

//    cqlStatement
    null
  }

  def createCqlStrategy(migrateeTable: MigrateeTable, session: Session, key: String, fetchLimit: Int): CqlStrategy = {
    val cqlStatement: CqlStatement = parseCqlStatement(migrateeTable.columns(key).valueSource)
    var eagerMap: mutable.Map[Seq[String], String] = mutable.Map[Seq[String], String]()

    val statement: Statement = new SimpleStatement("select * from " + cqlStatement.tableName)
    statement.setFetchSize(fetchLimit)
    var resultSet: ResultSet = session.execute(statement)
    var iterator = resultSet.iterator()

    while(iterator.hasNext) {
      var row: Row = iterator.next()
      var localKeys: mutable.MutableList[String] = new mutable.MutableList[String]()

      for(i<-0 until cqlStatement.keys.size) {
        localKeys += row.getObject(cqlStatement.keys(i)).toString
      }
      eagerMap += (localKeys -> row.getObject(cqlStatement.value).toString)
    }

    var eagerFetch: EagerFetch = new EagerFetch()
    eagerFetch.eagerMap = eagerMap
    eagerFetch.keys = cqlStatement.findKeys

    var cqlStrategy: CqlStrategy = new CqlStrategy(migrateeTable.mappedTableName)
    cqlStrategy.fetchType = eagerFetch

    cqlStrategy
  }
}

