package com.chrisomeara.pillar

import java.util.Date
import com.datastax.driver.core.{Session, Cluster}
import com.datastax.driver.core.exceptions.AlreadyExistsException

class CassandraMigrator(registry: Registry) extends Migrator {
  override def migrate(dataStore: DataStore, dateRestriction: Option[Date] = None) {
    withCluster(dataStore) { cluster =>
      migrate(cluster.connect(dataStore.keyspace), dateRestriction)
    }
  }

  override def migrate(session: Session, dateRestriction: Option[Date]) {
    val appliedMigrations = AppliedMigrations(session, registry)

    selectMigrationsToReverse(dateRestriction, appliedMigrations).foreach(_.executeDownStatement(session))
    selectMigrationsToApply(dateRestriction, appliedMigrations).foreach(_.executeUpStatement(session))
  }

  override def initialize(dataStore: DataStore, replicationOptions: ReplicationOptions = ReplicationOptions.default) {
    withCluster(dataStore) { cluster =>
      initialize(cluster.connect(), dataStore.keyspace, replicationOptions)
    }
  }

  override def initialize(session: Session, keyspace: String, replicationOptions: ReplicationOptions) {
    executeIdempotentCommand(session, s"CREATE KEYSPACE $keyspace WITH replication = $replicationOptions")
    executeIdempotentCommand(session,
      s"""
        | CREATE TABLE $keyspace.applied_migrations (
        |   authored_at timestamp,
        |   description text,
        |   applied_at timestamp,
        |   PRIMARY KEY (authored_at, description)
        |  )
      """.stripMargin
    )
  }

  private def executeIdempotentCommand(session: Session, statement: String) {
    try {
      session.execute(statement)
    } catch {
      case _: AlreadyExistsException =>
    }
  }

  override def destroy(dataStore: DataStore) {
    withCluster(dataStore) { cluster =>
      destroy(cluster.connect(), dataStore.keyspace)
    }
  }

  override def destroy(session: Session, keyspace: String) {
    session.execute(s"DROP KEYSPACE $keyspace")
  }

  private def selectMigrationsToApply(dateRestriction: Option[Date], appliedMigrations: AppliedMigrations): Seq[Migration] = {
    (dateRestriction match {
      case None => registry.all
      case Some(cutOff) => registry.authoredBefore(cutOff)
    }).filter(!appliedMigrations.contains(_))
  }

  private def selectMigrationsToReverse(dateRestriction: Option[Date], appliedMigrations: AppliedMigrations): Seq[Migration] = {
    (dateRestriction match {
      case None => List.empty[Migration]
      case Some(cutOff) => appliedMigrations.authoredAfter(cutOff)
    }).sortBy(_.authoredAt).reverse
  }

  private def withCluster(dataStore: DataStore)(block: Cluster => Unit) {
    val cluster = Cluster.builder().addContactPoint(dataStore.seedAddress).build()
    try {
      block(cluster)
    } finally {
      cluster.close
    }
  }

}