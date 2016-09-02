package com.chrisomeara.pillar

class IrreversibleMigrationException(migration: IrreversibleMigration)
  extends RuntimeException(s"Migration ${migration.authoredAt.getTime}: ${migration.description} is not reversible")
