package com.streamsend.pillar

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Metadata, Session}
import org.scalatest.matchers.ShouldMatchers

trait AcceptanceAssertions extends ShouldMatchers {
  val session: Session
  val keyspaceName: String

  protected def assertEmptyAppliedMigrationsTable() {
    session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(0)
  }

  protected def assertKeyspaceDoesNotExist() {
    val metadata: Metadata = session.getCluster.getMetadata
    metadata.getKeyspace(keyspaceName) should be(null)
  }
}
