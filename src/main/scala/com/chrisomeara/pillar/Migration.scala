package com.chrisomeara.pillar

import java.util.Date

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder

import scala.sys.process.Process

object Migration {
  def apply(description: String, authoredAt: Date, up: Seq[String], mapping: Seq[MigrateeTable]): Migration = {
    new IrreversibleMigration(description, authoredAt, up, mapping)
  }

  def apply(description: String, authoredAt: Date, up: Seq[String], mapping: Seq[MigrateeTable], down: Option[Seq[String]]): Migration = {
    down match {
      case Some(downStatement) =>
        new ReversibleMigration(description, authoredAt, up, mapping, downStatement)
      case None =>
        new ReversibleMigrationWithNoOpDown(description, authoredAt, up, mapping)
    }
  }
}

trait Migration {
  val description: String
  val authoredAt: Date
  val up: Seq[String]
  val mapping: Seq[MigrateeTable]

  def key: MigrationKey = MigrationKey(authoredAt, description)

  def authoredAfter(date: Date): Boolean = {
    authoredAt.after(date)
  }

  def authoredBefore(date: Date): Boolean = {
    authoredAt.compareTo(date) <= 0
  }

  def executeUpStatement(session: Session) {
    up.foreach(session.execute)
    insertIntoAppliedMigrations(session)

    executeTableStatement(session)
  }

  def executeTableStatement(session: Session): Unit = {
    //Maybe, these for loops can be reduce a function
    for(i <- mapping) {
      val s = session.execute("select column_name " +
        "from system.schema_columns " +
        "where keyspace_name ='" + session.getLoggedKeyspace + "' and columnfamily_name = '" + i.tableName + "'")

      var iterator = s.iterator()
      while(iterator.hasNext) {
        var columnName : Array[String] = iterator.next.toString.split("Row\\[|\\]")
        i.tableColumnList += columnName(1)
      }
    }

    for(i <- mapping) {
      val s = session.execute("select column_name " +
        "from system.schema_columns " +
        "where keyspace_name ='" + session.getLoggedKeyspace + "' and columnfamily_name = '" + i.mappedTableName + "'")

      var iterator = s.iterator()
      while(iterator.hasNext) {
        var columnName : Array[String] = iterator.next.toString.split("Row\\[|\\]")
        i.mappedTableColumnList += columnName(1)
      }
    }

    //create batch statements for each table
    for(i <- mapping) {
      //create batch statement
      var insert : String = "BEGIN BATCH "

      var resultSet : ResultSet = session.execute("select * from " + i.mappedTableName)
      var iterator = resultSet.iterator()

      while(iterator.hasNext) {
        var row : Row = iterator.next()
        insert += "INSERT INTO " + i.tableName + " ";

        //add column names
        insert += "("
        for(c <- i.tableColumnList) {
          insert += c + ","
        }
        insert = insert.substring(0,insert.size-1) //delete last comma
        insert += ") VALUES ("

        //find values each column
        for(c <- i.tableColumnList) {
          //respectively, sh-sql-default-null / will change as match case
          if(i.columnValueSource.contains(c)) { //sh or sql
            if(i.columnValueSource.get(c).toString().contains(".sh")) { //from sh file
              var resource : String = i.columnValueSource.get(c).get
              val arr : Array[String] = resource.split(" ")
              var processSh : String = "sh " + arr(0) //add path

              for(j <- 1 to arr.size-1) {
                if(arr(j).contains("$")) { //variable parameter
                  println("from query")
                  var parameter : Array[String] = arr(j).split("\\$")

                  processSh += " " + row.getObject(parameter(1))
                  println(processSh)
                }
                else {
                  println("normal")
                  processSh += " " + arr(j)
                }
              }
              try {
                val columnDataType : String = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace).getTable(i.tableName).getColumn(c).getType().toString
                if(columnDataType.contains("text") || columnDataType.contains("ascii") || columnDataType.contains("varchar")) {
                  val result :String = Process(processSh).!!
                  insert += "'"+ result + "',"
                }
                else {
                  try {
                    var result = Process(processSh).!!
                    insert += result + ","
                  } catch {
                    case e : NumberFormatException => println("Did not come an int value from " + processSh)
                  }

                }
              } catch {
                case e : Exception => println(e)
              }
            }
            else { //from sql query
              var query : String = i.columnValueSource.get(c).get.toString()
              if(query.contains("$")) {
                  val pattern = "\\$[a-z]*".r

                  for(m <- pattern.findAllIn(query)) { //replace variables with their real value
                    var realValue = row.getObject(m.substring(1, m.size))
                    query = pattern.replaceFirstIn(query, realValue.toString)
                  }
                }

              var result = session.execute(query).one().getObject(c)

              val columnDataType : String = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace).getTable(i.tableName).getColumn(c).getType().toString
              if(columnDataType.contains("text") || columnDataType.contains("ascii") || columnDataType.contains("varchar")) {
                insert += "'"+ result + "',"
              }
              else {
                insert += result + ","
              }
            }
          }
          else { //default value
            try {
              var result = row.getObject(c)

              val columnDataType : String = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace).getTable(i.tableName).getColumn(c).getType().toString
              if(columnDataType.contains("text") || columnDataType.contains("ascii") || columnDataType.contains("varchar")) {
                insert += "'"+ result + "',"
              }
              else {
                insert += result + ","
              }
            } catch {
              case e : Exception => {
                var result = "null";
                insert += result + ",";
              }
            }
          }
        }

        insert = insert.substring(0,insert.size-1) //delete last comma
        insert += ");"
      }
      //run the batch statement
      insert += " APPLY BATCH;"
      session.execute(insert)
    }
  }

  def executeDownStatement(session: Session)

  protected def deleteFromAppliedMigrations(session: Session) {
    session.execute(QueryBuilder.
      delete().
      from("applied_migrations").
      where(QueryBuilder.eq("authored_at", authoredAt)).
      and(QueryBuilder.eq("description", description))
    )
  }

  private def insertIntoAppliedMigrations(session: Session) {
    session.execute(QueryBuilder.
      insertInto("applied_migrations").
      value("authored_at", authoredAt).
      value("description", description).
      value("applied_at", System.currentTimeMillis())
    )
  }
}

class IrreversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[MigrateeTable]) extends Migration {
  def executeDownStatement(session: Session) {
    throw new IrreversibleMigrationException(this)
  }
}

class ReversibleMigrationWithNoOpDown(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[MigrateeTable]) extends Migration {
  def executeDownStatement(session: Session) {
    deleteFromAppliedMigrations(session)
  }
}

class ReversibleMigration(val description: String, val authoredAt: Date, val up: Seq[String], val mapping: Seq[MigrateeTable], val down: Seq[String]) extends Migration {
  def executeDownStatement(session: Session) {
    down.foreach(session.execute)
    deleteFromAppliedMigrations(session)
  }
}