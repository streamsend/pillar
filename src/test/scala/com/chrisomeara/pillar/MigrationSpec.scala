package com.chrisomeara.pillar

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import java.util.Date
import org.scalatest.mock.MockitoSugar

class MigrationSpec extends FunSpec with ShouldMatchers with MockitoSugar {
  describe(".apply") {
    describe("without a down parameter") {
      it("returns an irreversible migration") {
        Migration.apply("description", new Date(), Seq("up")).getClass should be(classOf[IrreversibleMigration])
      }
    }

    describe("with a down parameter") {
      describe("when the down is None") {
        it("returns a reversible migration with no-op down") {
          Migration.apply("description", new Date(), Seq("up"), None).getClass should be(classOf[ReversibleMigrationWithNoOpDown])
        }
      }

      describe("when the down is Some") {
        it("returns a reversible migration with no-op down") {
          Migration.apply("description", new Date(), Seq("up"), Some(Seq("down"))).getClass should be(classOf[ReversibleMigration])
        }
      }
    }

    describe("with a mapping parameter") {
      it("returns a irreversible modifiable migration") {
        Migration.apply("description", new Date(), "fetch", Seq("up"), Seq(new MigrateeTable))
      }
    }
  }
}
