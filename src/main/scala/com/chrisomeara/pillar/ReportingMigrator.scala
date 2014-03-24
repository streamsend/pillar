package com.chrisomeara.pillar

import java.util.Date
import com.datastax.driver.core.Session

class ReportingMigrator(reporter: Reporter, wrapped: Migrator) extends Migrator {
  def initialize(dataStore: DataStore, replicationOptions: ReplicationOptions) {
    reporter.initializing(dataStore, replicationOptions)
    wrapped.initialize(dataStore, replicationOptions)
  }

  def initialize(session: Session, keyspace: String, replicationOptions: ReplicationOptions) {
    reporter.initializing(session, keyspace, replicationOptions)
    wrapped.initialize(session, keyspace, replicationOptions)
  }

  def migrate(dataStore: DataStore, dateRestriction: Option[Date]) {
    reporter.migrating(dataStore, dateRestriction)
    wrapped.migrate(dataStore, dateRestriction)
  }

  def migrate(session: Session, dateRestriction: Option[Date]) {
    reporter.migrating(session, dateRestriction)
    wrapped.migrate(session, dateRestriction)
  }

  def destroy(dataStore: DataStore) {
    reporter.destroying(dataStore)
    wrapped.destroy(dataStore)
  }

  def destroy(session: Session, keyspace: String) {
    reporter.destroying(session, keyspace)
    wrapped.destroy(session, keyspace)
  }
}