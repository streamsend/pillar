package com.chrisomeara.pillar

import java.io.PrintStream
import java.util.Date
import com.datastax.driver.core.Session

class PrintStreamReporter(stream: PrintStream) extends com.chrisomeara.pillar.Reporter {
  def initializing(dataStore: DataStore, replicationOptions: ReplicationOptions) {
    stream.println(s"Initializing ${dataStore.name} data store")
  }

  def initializing(session: Session, keyspace: String, replicationOptions: ReplicationOptions) {
    stream.println(s"Initializing keyspace $keyspace")
  }

  def migrating(dataStore: DataStore, dateRestriction: Option[Date]) {
    stream.println(s"Migrating ${dataStore.name} data store")
  }

  def migrating(session: Session, dateRestriction: Option[Date]) {
    stream.println(s"Migrating keyspace ${session.getLoggedKeyspace}")
  }

  def applying(migration: Migration) {
    stream.println(s"Applying ${migration.authoredAt.getTime}: ${migration.description}")
  }

  def reversing(migration: Migration) {
    stream.println(s"Reversing ${migration.authoredAt.getTime}: ${migration.description}")
  }

  def destroying(dataStore: DataStore) {
    stream.println(s"Destroying ${dataStore.name} data store")
  }

  def destroying(session: Session, keyspace: String) {
    stream.println(s"Destroying keyspace $keyspace")
  }
}