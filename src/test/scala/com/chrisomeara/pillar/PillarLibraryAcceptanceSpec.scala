package com.chrisomeara.pillar

import java.util.Date

import com.chrisomeara.pillar.modify.{CqlStrategy, NoModify, ShStrategy}
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.exceptions.{InvalidQueryException, OperationTimedOutException}
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.scalatest.{BeforeAndAfter, FeatureSpec, GivenWhenThen, Matchers}

import scala.collection.mutable

class PillarLibraryAcceptanceSpec extends FeatureSpec with GivenWhenThen with BeforeAndAfter with Matchers with AcceptanceAssertions {
  val seedAddress = sys.env.getOrElse("PILLAR_SEED_ADDRESS", "127.0.0.1")
  val username = sys.env.getOrElse("PILLAR_USERNAME", "cassandra")
  val password = sys.env.getOrElse("PILLAR_PASSWORD", "cassandra")
  val port = sys.env.getOrElse("PILLAR_PORT", "9042").toInt
  val cluster = Cluster.builder().addContactPoint(seedAddress).withPort(port).withCredentials(username, password).build()
  val keyspaceName = "test_%d".format(System.currentTimeMillis())
  val session = cluster.connect()

  var mappingTables: Seq[MigrateeTable] = new mutable.MutableList[MigrateeTable]()
  var mappingTables_lazy: Seq[MigrateeTable] = new mutable.MutableList[MigrateeTable]()

  var migrateeTable: MigrateeTable = new MigrateeTable
  migrateeTable.columns = mutable.Map[String, ColumnProperty]()

  migrateeTable.tableName = "test_person"
  migrateeTable.mappedTableName = "person"
  migrateeTable.primaryKeyColumns += "name"

  var columnProperty = new ColumnProperty("name")
  columnProperty.dataType = "text"
  columnProperty.valueSource = "/home/mgunes/yenipersonname.sh $name deneme"
  columnProperty.modifyOperation = new ShStrategy
  migrateeTable.columns += ("name" -> columnProperty)

  columnProperty = new ColumnProperty("point")
  columnProperty.dataType = "int"
  columnProperty.valueSource = "select point from customer where name = '$name'"
  columnProperty.modifyOperation = new CqlStrategy(migrateeTable.mappedTableName)
  migrateeTable.columns += ("point" -> columnProperty)

  columnProperty = new ColumnProperty("surname")
  columnProperty.dataType = "text"
  columnProperty.valueSource = "no-source"
  columnProperty.modifyOperation = new NoModify
  migrateeTable.columns += ("point" -> columnProperty)

  columnProperty = new ColumnProperty("city")
  columnProperty.dataType = "text"
  columnProperty.valueSource = "no-source"
  columnProperty.modifyOperation = new NoModify
  migrateeTable.columns += ("point" -> columnProperty)

  migrateeTable.mappedTableColumns = new mutable.MutableList[String]()
  migrateeTable.mappedTableColumns += "name"
  migrateeTable.mappedTableColumns += "surname"
  migrateeTable.mappedTableColumns += "age"

  mappingTables = List(migrateeTable)

  val queries: Seq[String] = List("create table person ( name text, surname text, age int, primary key(age, name, surname))",
    "create table customer( name text, age int, point int, primary key(name))",
    "create table test_person ( name text, surname text, point int, city text, primary key(name))",
    "insert into person (name, surname, age) values ('celebi', 'murat', 28)",
    "insert into person (name, surname, age) values ('mustafa', 'gunes', 24)",
    "insert into person (name, surname, age) values ('ali', 'yildiz', 25)",
    "insert into person (name, surname, age) values ('ayse', 'yilmaz', 26)",
    "insert into person (name, surname, age) values ('fatma', 'gun', 27)",
    "insert into customer (name, age, point) values ('fatma', 27, 115)",
    "insert into customer (name, age, point) values ('ayse', 26, 156)",
    "insert into customer (name, age, point) values ('ali', 25, 108)",
    "insert into customer (name, age, point) values ('mustafa', 24, 111)"
  )

  var migrateeTable_Lazy = new MigrateeTable
  migrateeTable_Lazy.tableName = "test_person_lazy"
  migrateeTable_Lazy.mappedTableName = "person_lazy"
  migrateeTable_Lazy.primaryKeyColumns += "name"

  var columnProperty_lazy = new ColumnProperty("name")
  columnProperty_lazy.dataType = "text"
  columnProperty_lazy.valueSource = "/home/mgunes/IdeaProjects/pillar/src/test/resources/pillar/test_person_name.sh $name test"
  columnProperty_lazy.modifyOperation = new ShStrategy
  migrateeTable_Lazy.columns += ("name" -> columnProperty)

  columnProperty_lazy = new ColumnProperty("surname")
  columnProperty_lazy.dataType = "text"
  columnProperty_lazy.valueSource = "select surname from person_lazy where name = '$name'"
  columnProperty_lazy.modifyOperation = new CqlStrategy(migrateeTable.mappedTableName)
  migrateeTable_Lazy.columns += ("surname" -> columnProperty)

  migrateeTable_Lazy.mappedTableColumns = new mutable.MutableList[String]()
  migrateeTable_Lazy.mappedTableColumns += "name"
  migrateeTable_Lazy.mappedTableColumns += "surname"
  migrateeTable_Lazy.mappedTableColumns += "age"

  mappingTables_lazy = List(migrateeTable_Lazy)

  val queries_lazy: Seq[String] = List("create table person_lazy (name text,surname text,age int,primary key(age, name, surname))",
    "create table test_person_lazy (name text, surname text, primary key(name))",
    "insert into person (name, surname, age) values ('mustafa', 'gunes', 24)",
    "insert into person (name, surname, age) values ('ali', 'yildiz', 25)",
    "insert into person (name, surname, age) values ('ayse', 'yilmaz', 26)",
    "insert into person (name, surname, age) values ('fatma', 'gun', 27)",
    "insert into person (name, surname, age) values ('celebi', 'murat', 28)"
  )

  val migrations = Seq(
    Migration("creates events table", new Date(System.currentTimeMillis() - 5000),
      Seq("""
        |CREATE TABLE events (
        |  batch_id text,
        |  occurred_at uuid,
        |  event_type text,
        |  payload blob,
        |  PRIMARY KEY (batch_id, occurred_at, event_type)
        |)
      """.stripMargin)),
    Migration("creates views table", new Date(System.currentTimeMillis() - 3000),
      Seq("""
        |CREATE TABLE views (
        |  id uuid PRIMARY KEY,
        |  url text,
        |  person_id int,
        |  viewed_at timestamp
        |)
      """.stripMargin),
      Some( Seq("""
              |DROP TABLE views
            """.stripMargin))),
    Migration("adds user_agent to views table", new Date(System.currentTimeMillis() - 1000),
      Seq("""
        |ALTER TABLE views
        |ADD user_agent text
      """.stripMargin), None), // Dropping a column is coming in Cassandra 2.0
    Migration("adds index on views.user_agent", new Date(),
      Seq("""
        |CREATE INDEX views_user_agent ON views(user_agent)
      """.stripMargin),
      Some( Seq("""
              |DROP INDEX views_user_agent
            """.stripMargin))),
    Migration("modify and migrate", new Date(), "eager",
      queries, mappingTables
    ),
    Migration("modify and migrate with lazy", new Date(), "lazy",
      queries_lazy, mappingTables_lazy
    )
  )
  val registry = Registry(migrations)
  val migrator = Migrator(registry)

  after {
    try {
      session.execute("DROP KEYSPACE %s".format(keyspaceName))
    } catch {
      case ok: InvalidQueryException =>
      case te: OperationTimedOutException => {
      /*  if(session.execute("SELECT * FROM system_schema.keyspaces where keyspace_name = '" + keyspaceName + "';").all().size() == 1)
          te.printStackTrace()*/
      }
    }
  }

  feature("The operator can initialize a keyspace") {
    info("As an application operator")
    info("I want to initialize a Cassandra keyspace")
    info("So that I can manage the keyspace schema")

    scenario("initialize a non-existent keyspace") {
      Given("a non-existent keyspace")

      When("the migrator initializes the keyspace")
      migrator.initialize(session, keyspaceName)

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }

    scenario("initialize an existing keyspace without a applied_migrations column family") {
      Given("an existing keyspace")
      session.execute("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}".format(keyspaceName))

      When("the migrator initializes the keyspace")
      migrator.initialize(session, keyspaceName)

      Then("the keyspace contains a applied_migrations column family")
      assertEmptyAppliedMigrationsTable()
    }

    scenario("initialize an existing keyspace with a applied_migrations column family") {
      Given("an existing keyspace")
      migrator.initialize(session, keyspaceName)

      When("the migrator initializes the keyspace")
      migrator.initialize(session, keyspaceName)

      Then("the migration completes successfully")
    }
  }

  feature("The operator can destroy a keyspace") {
    info("As an application operator")
    info("I want to destroy a Cassandra keyspace")
    info("So that I can clean up automated tasks")

    scenario("destroy a keyspace") {
      Given("an existing keyspace")
      migrator.initialize(session, keyspaceName)

      When("the migrator destroys the keyspace")
      migrator.destroy(session, keyspaceName)

      Then("the keyspace no longer exists")
      assertKeyspaceDoesNotExist()
    }

    scenario("destroy a bad keyspace") {
      Given("a datastore with a non-existing keyspace")

      When("the migrator destroys the keyspace")

      Then("the migrator throws an exception")
      evaluating {
        migrator.destroy(session, keyspaceName)
      } should produce[Throwable]
    }
  }

  feature("The operator can apply migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from an older version of the schema to a newer version")
    info("So that I can run an application using the schema")

    scenario("all migrations") {
      Given("an initialized, empty, keyspace")
      migrator.initialize(session, keyspaceName)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates the schema")
      migrator.migrate(cluster.connect(keyspaceName))

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.select().from(keyspaceName, "events")).all().size() should equal(0)

      And("the keyspace contains the views table")
      session.execute(QueryBuilder.select().from(keyspaceName, "views")).all().size() should equal(0)

      And("the applied_migrations table records the migrations")
      session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(6)
    }

    scenario("some migrations") {
      Given("an initialized, empty, keyspace")
      migrator.initialize(session, keyspaceName)

      Given("a migration that creates an events table")
      Given("a migration that creates a views table")

      When("the migrator migrates with a cut off date")
      migrator.migrate(cluster.connect(keyspaceName), Some(migrations(0).authoredAt))

      Then("the keyspace contains the events table")
      session.execute(QueryBuilder.select().from(keyspaceName, "events")).all().size() should equal(0)

      And("the applied_migrations table records the migration")
      session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(1)
    }

    scenario("skip previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(session, keyspaceName)

      Given("a set of migrations applied in the past")
      migrator.migrate(cluster.connect(keyspaceName))

      When("the migrator applies migrations")
      migrator.migrate(cluster.connect(keyspaceName))

      Then("the migration completes successfully")
    }
  }

  feature("The operator can reverse migrations") {
    info("As an application operator")
    info("I want to migrate a Cassandra keyspace from a newer version of the schema to an older version")
    info("So that I can run an application using the schema")

    scenario("reversible previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(session, keyspaceName)

      Given("a set of migrations applied in the past")
      migrator.migrate(cluster.connect(keyspaceName))

      When("the migrator migrates with a cut off date")
      migrator.migrate(cluster.connect(keyspaceName), Some(migrations(0).authoredAt))

      Then("the migrator reverses the reversible migration")
      val thrown = intercept[InvalidQueryException] {
        session.execute(QueryBuilder.select().from(keyspaceName, "views")).all()
      }
      thrown.getMessage should include("views")

      And("the migrator removes the reversed migration from the applied migrations table")
      val reversedMigration = migrations(1)
      val query = QueryBuilder.
        select().
        from(keyspaceName, "applied_migrations").
        where(QueryBuilder.eq("authored_at", reversedMigration.authoredAt)).
        and(QueryBuilder.eq("description", reversedMigration.description))
      session.execute(query).all().size() should equal(0)
    }

    scenario("irreversible previously applied migration") {
      Given("an initialized keyspace")
      migrator.initialize(session, keyspaceName)

      Given("a set of migrations applied in the past")
      migrator.migrate(cluster.connect(keyspaceName))

      When("the migrator migrates with a cut off date")
      val thrown = intercept[IrreversibleMigrationException] {
        migrator.migrate(cluster.connect(keyspaceName), Some(new Date(0)))
      }

      Then("the migrator reverses the reversible migrations")
      session.execute(QueryBuilder.select().from(keyspaceName, "applied_migrations")).all().size() should equal(3)

      And("the migrator throws an IrreversibleMigrationException")
      thrown should not be null
    }
  }
}