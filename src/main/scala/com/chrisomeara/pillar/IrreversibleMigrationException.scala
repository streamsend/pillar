package com.chrisomeara.pillar

class IrreversibleMigrationException(migration: IrreversibleMigration)
  extends RuntimeException(s"Migration ${migration.authoredAt.getTime}: ${migration.description} is not reversible")

class IrreversibleModifiableMigrationException(migration: IrreversibleModifiableMigration)
  extends RuntimeException(s"Migration ${migration.authoredAt.getTime}: ${migration.description} is not reversible")

class ReversibleMigrationWithNoOpDownException(migration: ReversibleMigrationWithNoOpDown)
  extends RuntimeException(s"Migration ${migration.authoredAt.getTime}: ${migration.description} is not reversible")

class ReversibleMigrationException(migration: ReversibleMigration)
  extends RuntimeException(s"Migration ${migration.authoredAt.getTime}: ${migration.description} is not reversible")