package com.chrisomeara.pillar

import com.chrisomeara.pillar.modify.{CqlStrategy, EagerFetch}
import com.datastax.driver.core._

import scala.collection.mutable

/**
  * Created by mgunes on 15.08.2016.
  */
case class CqlStatement(val columnName: String, val tableName: String, val keys: Seq[String], val findKeys: mutable.MutableList[String])

object CqlStatement {
  def parseCqlStatement(query: String): CqlStatement = {
    val pattern = "((( )+(in))|([a-zA-Z0-9_]+( )+=))( )+'?\\$[a-z]+'?".r

    var keys: mutable.MutableList[String] = new mutable.MutableList[String]()
    var findKeys: mutable.MutableList[String] = new mutable.MutableList[String]()

    val matches = pattern.findAllIn(query)

    matches.foreach((xx: String) => {
      if(xx.contains("=")) {
        val arr: Array[String] = xx.split("( )+=( )+")
        keys += arr(0).trim
        findKeys += arr(1).trim
      }
      //else if(xx.contains("in"))
    })

    val patternForNames = "(select)( )+[a-zA-Z0-9_]+( )+(from)( )+[a-zA-Z0-9_]+( )+".r
    val matches2: String = patternForNames.findFirstIn(query).get
    val arr: Array[String] = matches2.split("(select|from)")
    val cqlStatement: CqlStatement = new CqlStatement(arr(1).trim, arr(2).trim, keys, findKeys)

    cqlStatement
  }

  def createCqlStrategy(migrateeTable: MigrateeTable, session: Session, key: String, fetchLimit: Int): CqlStrategy = {
    val cqlStatement: CqlStatement = parseCqlStatement(migrateeTable.columns(key).valueSource)
    var eagerMap: mutable.Map[Seq[String], AnyRef] = mutable.Map[Seq[String], AnyRef]()

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
      eagerMap += (localKeys -> row.getObject(cqlStatement.columnName))
    }

    var eagerFetch: EagerFetch = new EagerFetch()
    eagerFetch.eagerMap = eagerMap
    eagerFetch.keys = cqlStatement.findKeys

    var cqlStrategy: CqlStrategy = new CqlStrategy(migrateeTable.mappedTableName)
    cqlStrategy.fetchType = eagerFetch

    cqlStrategy
  }
}

