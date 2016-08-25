package com.chrisomeara.pillar

import java.io.InputStream
import java.util.Date

import com.chrisomeara.pillar.modify.{ShStrategy, CqlStrategy}

import scala.collection.mutable
import scala.io.Source

object Parser {
  def apply(): Parser = new Parser

  private val MatchAttribute = """^-- (authoredAt|description|fetch|up|down|stage|mapping|table|end):(.*)$""".r
}

class PartialMigration {
  var description: String = ""
  var authoredAt: String = ""
  var fetch: String = ""

  var upStages = new mutable.MutableList[String]()
  var downStages : Option[mutable.MutableList[String]] = None

  var currentUp = new mutable.MutableList[String]()
  var currentDown: Option[mutable.MutableList[String]] = None

  var currentColumn = new mutable.MutableList[String]()

  var mapping = new mutable.MutableList[MigrateeTable]()

  def rotateUp() = {
    upStages += currentUp.mkString("\n")
    upStages = upStages.filterNot(line => line.isEmpty)
    currentUp = new mutable.MutableList[String]()
  }

  def rotateDown() = {
    currentDown match {
      case Some(currentDownLines) =>
        downStages match {
          case None => downStages = Some(new mutable.MutableList[String]())
          case Some(_) =>
        }

        downStages = Some(downStages.get += currentDownLines.mkString("\n"))
      case None =>
    }

    currentDown = None
  }

  def validate: Option[Map[String, String]] = {

    rotateUp()
    rotateDown()

    val errors = mutable.Map[String, String]()

    if (description.isEmpty) errors("description") = "must be present"
    if (authoredAt.isEmpty) errors("authoredAt") = "must be present"
    if (!authoredAt.isEmpty && authoredAtAsLong < 1) errors("authoredAt") = "must be a number greater than zero"
    if (upStages.isEmpty) errors("up") = "must be present"
    if ((fetch.isEmpty && !mapping.isEmpty) || (!fetch.isEmpty && mapping.isEmpty)) errors("mapping-fetch") = "must be together"

    if (errors.nonEmpty) Some(errors.toMap) else None
  }

  def authoredAtAsLong: Long = {
    try {
      authoredAt.toLong
    } catch {
      case _:NumberFormatException => -1
    }
  }
}

class Parser {

  import Parser.MatchAttribute

  trait ParserState

  case object ParsingAttributes extends ParserState

  case object ParsingUp extends ParserState

  case object ParsingDown extends ParserState

  case object ParsingUpStage extends ParserState

  case object ParsingDownStage extends ParserState

  case object ParsingTable extends ParserState

  var migrateeTable : MigrateeTable = _

  def parse(resource: InputStream): Migration = {
    val inProgress = new PartialMigration
    var state: ParserState = ParsingAttributes
    Source.fromInputStream(resource).getLines().foreach {
      case MatchAttribute("authoredAt", authoredAt) =>
        inProgress.authoredAt = authoredAt.trim
      case MatchAttribute("description", description) =>
        inProgress.description = description.trim
      case MatchAttribute("fetch", fetch) =>
        inProgress.fetch = fetch.trim
      case MatchAttribute("up", _) =>
        state = ParsingUp
      case MatchAttribute("down", _) =>
        inProgress.rotateUp()
        inProgress.currentDown = Some(new mutable.MutableList[String]())
        state = ParsingDown
      case MatchAttribute("mapping", _) =>
        state = ParsingTable
      case MatchAttribute("stage", number) =>
        state match {
          case ParsingUp => state = ParsingUpStage
          case ParsingUpStage => inProgress.rotateUp()
          case ParsingDown => state = ParsingDownStage
          case ParsingDownStage => inProgress.rotateDown(); inProgress.currentDown = Some(new mutable.MutableList[String]())
        }
      case MatchAttribute("table", table) =>
        migrateeTable = new MigrateeTable()
        val arr : Array[String] = table.split("->")
        migrateeTable.tableName = arr(1).trim
        migrateeTable.mappedTableName = arr(0).trim
      case MatchAttribute("end", _) =>
        for(line <- inProgress.currentColumn) {
          var arr : Array[String] = line.split("->")
          try {
            var columnProperty = new ColumnProperty(arr(0))
            columnProperty.valueSource = arr(1)
            if(arr(1).matches("(select)( )*[a-z]*( )*(from)( )*[a-zA-Z0-9$'= ]*"))
              columnProperty.modifyOperation = new CqlStrategy(migrateeTable.mappedTableName)
            else if(arr(1).contains(".sh"))
              columnProperty.modifyOperation = new ShStrategy
            migrateeTable.columns += (arr(0) -> columnProperty)
          } catch {
            case e : Exception => println(e)
          }
        }
        inProgress.mapping.+=(migrateeTable)
        migrateeTable = null
        inProgress.currentColumn.clear()
      case cql =>
        if (!cql.isEmpty) {

          state match {
            case ParsingUp | ParsingUpStage => inProgress.currentUp += cql
            case ParsingDown | ParsingDownStage => inProgress.currentDown.get += cql
            case ParsingTable => inProgress.currentColumn += cql
            case other =>
          }
        }
    }

    inProgress.validate match {
      case Some(errors) => throw new InvalidMigrationException(errors)
      case None =>
        if(!inProgress.fetch.isEmpty)
          Migration(inProgress.description, new Date(inProgress.authoredAtAsLong), inProgress.fetch, inProgress.upStages, inProgress.mapping)
        else
          inProgress.downStages match {
            case Some(downLines) =>
              if (downLines.forall(line => line.isEmpty)) {
                Migration(inProgress.description, new Date(inProgress.authoredAtAsLong), inProgress.upStages, None)
              } else {
                Migration(inProgress.description, new Date(inProgress.authoredAtAsLong), inProgress.upStages, Some(downLines))
              }
            case None => Migration(inProgress.description, new Date(inProgress.authoredAtAsLong), inProgress.upStages)
          }
    }
  }
}
