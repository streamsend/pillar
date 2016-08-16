package com.chrisomeara.pillar

import org.scalatest.{FunSpec, BeforeAndAfter}
import org.scalatest.matchers.ShouldMatchers
import java.io.{ByteArrayInputStream, FileInputStream}
import java.util.Date

class ParserSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {
  describe("#parse") {
    describe("1370028262000_creates_events_table.cql") {
      val migrationPath = "src/test/resources/pillar/migrations/faker/1370028262000_creates_events_table.cql"

      it("returns a migration object") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).getClass should be(classOf[IrreversibleMigration])
      }

      it("assigns authoredAt") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).authoredAt should equal(new Date(1370023262000L))
      }

      it("assigns description") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).description should equal("creates events table")
      }

      it("assigns up") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).up should contain(
          """CREATE TABLE events (
            |  batch_id text,
            |  occurred_at uuid,
            |  event_type text,
            |  payload blob,
            |  PRIMARY KEY (batch_id, occurred_at, event_type)
            |)""".stripMargin)
      }
    }

    describe("1469630066000_creates_users_groups_table.cql") {
      val migrationPath = "src/test/resources/pillar/migrations/faker/1469630066000_creates_users_groups_table.cql"

      it("returns a migration object") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).getClass should be(classOf[ReversibleMigration])
      }

      it("assigns authoredAt") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).authoredAt should equal(new Date(1469630066000L))
      }

      it("assigns description") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).description should equal("creates users and groups tables")
      }

      it("assigns two up stages") {
        val resource = new FileInputStream(migrationPath)
        val migration = Parser().parse(resource)

        migration.up should contain(
          """CREATE TABLE groups (
            |  id uuid,
            |  name text,
            |  PRIMARY KEY (id)
            |)""".stripMargin)

        migration.up should contain(
          """CREATE TABLE users (
            |  id uuid,
            |  group_id uuid,
            |  username text,
            |  password text,
            |  PRIMARY KEY (id)
            |)""".stripMargin)
      }

      it("assigns two down stages") {
        val resource = new FileInputStream(migrationPath)
        val migration = Parser().parse(resource).asInstanceOf[ReversibleMigration]

        migration.down should contain("""DROP TABLE users""".stripMargin)
        migration.down should contain("""DROP TABLE groups""".stripMargin)
      }
    }

    describe("1370028263000_creates_views_table.cql") {
      val migrationPath = "src/test/resources/pillar/migrations/faker/1370028263000_creates_views_table.cql"

      it("returns a migration object") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).getClass should be(classOf[ReversibleMigration])
      }

      it("assigns down") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).asInstanceOf[ReversibleMigration].down should contain("DROP TABLE views")
      }
    }

    describe("1370028264000_adds_user_agent_to_views_table.cql") {
      val migrationPath = "src/test/resources/pillar/migrations/faker/1370028264000_adds_user_agent_to_views_table.cql"

      it("returns a migration object") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).getClass should be(classOf[ReversibleMigrationWithNoOpDown])
      }
    }

    describe("1370028265000_modify_values_person.cql") {
      val migrationPath = "src/test/resources/pillar/migrations/faker/1370028265000_modify_values_person.cql"

      it("returns a migration object") {
        val resource = new FileInputStream(migrationPath)
        Parser().parse(resource).getClass should be(classOf[IrreversibleModifiableMigration])
      }

      it("assigns fetch") {
        val resource = new FileInputStream(migrationPath)
        val migration = Parser().parse(resource).asInstanceOf[IrreversibleModifiableMigration]
        migration.fetch should equal ("eager")
      }

      it("assigns a mapping sections") {
        val resource = new FileInputStream(migrationPath)
        val migration = Parser().parse(resource).asInstanceOf[IrreversibleModifiableMigration]


        migration.mapping.size should equal(1)
        migration.mapping(0).tableName should equal("test_person")
        migration.mapping(0).columns.contains("point")
      }
    }

    describe("a migration missing an up stanza") {
      val migrationContent =
        """-- description: creates events table
          |-- authoredAt: 1370023262""".stripMargin

      it("raises an InvalidMigrationException") {
        val resource = new ByteArrayInputStream(migrationContent.getBytes)
        val thrown = intercept[InvalidMigrationException] {
          Parser().parse(resource)
        }
        thrown.errors("up") should equal("must be present")
      }
    }

    describe("a migration missing a description stanza") {
      val migrationContent = "-- authoredAt: 1370023262"

      it("raises an InvalidMigrationException") {
        val resource = new ByteArrayInputStream(migrationContent.getBytes)
        val thrown = intercept[InvalidMigrationException] {
          Parser().parse(resource)
        }
        thrown.errors("description") should equal("must be present")
      }
    }

    describe("a migration missing an authoredAt stanza") {
      val migrationContent = "-- description: creates events table"

      it("raises an InvalidMigrationException") {
        val resource = new ByteArrayInputStream(migrationContent.getBytes)
        val thrown = intercept[InvalidMigrationException] {
          Parser().parse(resource)
        }
        thrown.errors("authoredAt") should equal("must be present")
      }
    }

    describe("a migration with a bogus authored at stanza") {
      val migrationContent = "-- authoredAt: a long, long time ago"

      it("raises an InvalidMigrationException") {
        val resource = new ByteArrayInputStream(migrationContent.getBytes)
        val thrown = intercept[InvalidMigrationException] {
          Parser().parse(resource)
        }
        thrown.errors("authoredAt") should equal("must be a number greater than zero")
      }
    }

    describe("a migration missing association of fetch and mapping") {
      val migrationContent = "-- fetch: lazy"

      it("raises an InvalidMigrationException") {
        val resource = new ByteArrayInputStream(migrationContent.getBytes)
        val thrown = intercept[InvalidMigrationException] {
          Parser().parse(resource)
        }
        thrown.errors("mapping-fetch") should equal("must be together")
      }
    }
  }
}
