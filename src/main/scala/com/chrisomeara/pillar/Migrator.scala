package com.chrisomeara.pillar

import java.util.Date

object Migrator {
  def apply(registry: Registry): Migrator = {
    new CassandraMigrator(registry)
  }

  def apply(registry: Registry, reporter: Reporter): Migrator = {
    new ReportingMigrator(reporter, apply(registry))
  }
}

import com.datastax.driver.core.Session

trait Migrator {
  def migrate(dataStore: DataStore, dateRestriction: Option[Date] = None)

  /**
   * Run migrations using the provided session. The session must currently be logged in to the keyspace
   * where migrations shall be run.
   */
  def migrate(session: Session, dateRestriction: Option[Date])

  def initialize(dataStore: DataStore, replicationOptions: ReplicationOptions = ReplicationOptions.default)
  // No default replicationOptions again, because this would lead to an error
  // "multiple overloaded alternatives of method initialize define default arguments"
  def initialize(session: Session, keyspace: String, replicationOptions: ReplicationOptions)

  def destroy(dataStore: DataStore)
  def destroy(session: Session, keyspace: String)
}