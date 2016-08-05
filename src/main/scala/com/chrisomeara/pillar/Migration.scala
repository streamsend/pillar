package com.chrisomeara.pillar

import java.util.Date

import com.datastax.driver.core.{BatchStatement, PreparedStatement, Session, Statement}
import com.datastax.driver.core.querybuilder.QueryBuilder

object Migration {
  def apply(description: String, authoredAt: Date, up: Seq[String], mapping: Seq[String]): Migration = {
    new IrreversibleMigration(description, authoredAt, up, mapping)
  }

  def apply(description: String, authoredAt: Date, up: Seq[String], mapping: Seq[String], down: Option[Seq[String]]): Migration = {
    down match {
      case Some(downStatement) =>
        new ReversibleMigration(description, authoredAt, up, mapping, downStatement)
      case None =>
        new ReversibleMigrationWithNoOpDown(description, authoredAt, up, mapping)
    }
  }
}

trait Migration {
  val description: String
  val authoredAt: Date
  val up: Seq[String]
  val mapping: Seq[String]

  def key: MigrationKey = MigrationKey(authoredAt, description)

  def authoredAfter(date: Date): Boolean = {
    authoredAt.after(date)
  }

  def authoredBefore(date: Date): Boolean = {
    authoredAt.compareTo(date) <= 0
  }

  def executeUpStatement(session: Session) {
    up.foreach(session.execute)

    /*var asd: BatchStatement = BatchStatement
    asd.add(mapping.foreach(_))
    session.execute(asd)**/

    insertIntoAppliedMigrations(session)


  }

  def executeDownStatement(session: Session)

  protected def deleteFromAppliedMigrations(session: Session) {
    session.execute(QueryBuilder.
      delete().
      from("applied_migrations").
      where(QueryBuilder.eq("authored_at", authoredAt)).
      and(QueryBuilder.eq("description", description))
    )
  }

  private def insertIntoAppliedMigrations(session: Session) {
    session.execute(QueryBuilder.
      insertInto("applied_migrations").
      value("authored_at", authoredAt).
      value("description", description).
      value("applied_at", System.currentTimeMillis())
    )
  }
}

class IrreversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[String]) extends Migration {
  def executeDownStatement(session: Session) {
    throw new IrreversibleMigrationException(this)
  }
}

class ReversibleMigrationWithNoOpDown(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[String]) extends Migration {
  def executeDownStatement(session: Session) {
    deleteFromAppliedMigrations(session)
  }
}

class ReversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val down: Seq[String], val mapping: Seq[String]) extends Migration {
  def executeDownStatement(session: Session) {
    down.foreach(session.execute)
    deleteFromAppliedMigrations(session)
  }
}