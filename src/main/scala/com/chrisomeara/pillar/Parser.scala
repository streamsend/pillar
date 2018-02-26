package com.chrisomeara.pillar

import java.util.Date
import java.io.InputStream
import scala.collection.mutable
import scala.io.Source

object Parser {
  def apply(): Parser = new Parser

  private val MatchAttribute = """^-- (authoredAt|description|up|down|stage):(.*)$""".r
}

class PartialMigration {
  var description: String = ""
  var authoredAt: String = ""

  var upStages = new mutable.MutableList[String]()
  var downStages : Option[mutable.MutableList[String]] = None

  var currentUp = new mutable.MutableList[mutable.MutableList[String]]()
  var currentDown: Option[mutable.MutableList[String]] = None

  var lastLine = ""

  def rotateUp() = {
    upStages ++= currentUp.withFilter(_.nonEmpty).map(_.filter(_.nonEmpty).mkString("\n"))
    upStages = upStages.filterNot(line => line.isEmpty)
    lastLine = ""
    currentUp = new mutable.MutableList[mutable.MutableList[String]]()
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

  private def parseMultilineStatments(pm: PartialMigration, currentLine : String): Unit = {
    if(pm.lastLine.trim().isEmpty) pm.currentUp += new mutable.MutableList[String]()

    pm.currentUp.last += currentLine

    pm.lastLine = currentLine
  }

  def parse(resource: InputStream): Migration = {
    val inProgress = new PartialMigration
    var state: ParserState = ParsingAttributes
    Source.fromInputStream(resource).getLines().foreach {
      case MatchAttribute("authoredAt", authoredAt) =>
        inProgress.authoredAt = authoredAt.trim
      case MatchAttribute("description", description) =>
        inProgress.description = description.trim
      case MatchAttribute("up", _) =>
        state = ParsingUp
      case MatchAttribute("down", _) =>
        inProgress.rotateUp()
        inProgress.currentDown = Some(new mutable.MutableList[String]())
        state = ParsingDown
      case MatchAttribute("stage", number) =>
        state match {
          case ParsingUp => state = ParsingUpStage
          case ParsingUpStage => inProgress.rotateUp()
          case ParsingDown => state = ParsingDownStage
          case ParsingDownStage => inProgress.rotateDown(); inProgress.currentDown = Some(new mutable.MutableList[String]())
        }
      case cql =>
        (cql.isEmpty, state) match {
          case (_, ParsingUp | ParsingUpStage) => parseMultilineStatments(inProgress, cql)
          case (false, ParsingDown | ParsingDownStage) => inProgress.currentDown.get += cql
          case other =>
        }
    }
    inProgress.validate match {
      case Some(errors) => throw new InvalidMigrationException(errors)
      case None =>

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
