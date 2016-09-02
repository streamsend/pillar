# Pillar

Pillar manages migrations for your [Cassandra][cassandra] data stores.

[cassandra]:http://cassandra.apache.org

Pillar grew from a desire to automatically manage Cassandra schema as code. Managing schema as code enables automated
build and deployment, a foundational practice for an organization striving to achieve [Continuous Delivery][cd].

Pillar is to Cassandra what [Rails ActiveRecord][ar] migrations or [Play Evolutions][evolutions] are to relational
databases with one key difference: Pillar is completely independent from any application development framework.

[cd]:http://en.wikipedia.org/wiki/Continuous_delivery
[ar]:https://github.com/rails/rails/tree/master/activerecord
[evolutions]:http://www.playframework.com/documentation/2.0/Evolutions

## Installation

### Prerequisites

1. Java SE 6 or more recent runtime environment
1. Cassandra 2.x or 3.x

### From Source

This method requires [Simple Build Tool (sbt)][sbt].
Building an RPM also requires [Effing Package Management (fpm)][fpm].

    % sbt assembly   # builds just the jar file in the target/ directory

    % sbt rh-package # builds the jar and the RPM in the target/ directory
    % sudo rpm -i target/pillar-1.0.0-DEV.noarch.rpm

The RPM installs Pillar to /opt/pillar.

[sbt]:http://www.scala-sbt.org
[fpm]:https://github.com/jordansissel/fpm

### Packages

Pillar is available at Maven Central under the GroupId com.chrisomeara and ArtifactId pillar_2.10 or pillar_2.11. The current version is 2.3.0.

#### sbt

  libraryDependencies += "com.chrisomeara" % "pillar_2.10" % "2.3.0"

#### Gradle

  compile 'com.chrisomeara:pillar_2.10:2.3.0'

## Usage

### Terminology

Data Store
: A logical grouping of environments. You will likely have one data store per application.

Environment
: A context or grouping of settings for a single data store. You will likely have at least development and production
environments for each data store.

Migration
: A single change to a data store. Migrations have a description and a time stamp indicating the time at which it was
authored. Migrations are applied in ascending order and reversed in descending order.

### Command Line

Here's the short version:

  1. Write migrations, place them in conf/pillar/migrations/myapp.
  1. Add pillar settings to conf/application.conf.
  1. % pillar -e development initialize myapp
  1. % pillar -e development migrate myapp

#### Migration Files

Migration files contain metadata about the migration, a [CQL][cql] statement used to apply the migration and,
optionally, a [CQL][cql] statement used to reverse the migration. Each file describes one migration. You probably
want to name your files according to time stamp and description, 1370028263000_creates_views_table.cql, for example.
Pillar reads and parses all files in the migrations directory, regardless of file name.

[cql]:http://cassandra.apache.org/doc/cql3/CQL.html

Pillar supports reversible, irreversible and reversible with a no-op down statement migrations. Here are examples of
each:

Reversible migrations have up and down properties.

    -- description: creates views table
    -- authoredAt: 1370028263000
    -- up:

    CREATE TABLE views (
      id uuid PRIMARY KEY,
      url text,
      person_id int,
      viewed_at timestamp
    )

    -- down:

    DROP TABLE views

Irreversible migrations have an up property but no down property.

    -- description: creates events table
    -- authoredAt: 1370023262000
    -- up:

    CREATE TABLE events (
      batch_id text,
      occurred_at uuid,
      event_type text,
      payload blob,
      PRIMARY KEY (batch_id, occurred_at, event_type)
    )

Reversible migrations with no-op down statements have an up property and an empty down property.

    -- description: adds user_agent to views table
    -- authoredAt: 1370028264000
    -- up:

    ALTER TABLE views
    ADD user_agent text

    -- down:

Each migration may optionally specify multiple stages. Stages are executed in the order specified.

    -- description: creates users and groups tables
    -- authoredAt: 1469630066000
    -- up:

    -- stage: 1
    CREATE TABLE groups (
      id uuid,
      name text,
      PRIMARY KEY (id)
    )

    -- stage: 2
    CREATE TABLE users (
      id uuid,
      group_id uuid,
      username text,
      password text,
      PRIMARY KEY (id)
    )


    -- down:

    -- stage: 1
    DROP TABLE users

    -- stage: 2
    DROP TABLE groups

To carry values (without any change or after modifying) from a table to a new table, you can use "-- mapping" section.
"-- table" section is necessary for each table. Name before '->' is migrating table and name after '->'  is migratee table.
You can decide whether the values will change or not. New values may come from an sh file or an cql query. You can pass parameters via '$'. 
These parameters must be a column name on migrating table. If you want to use constant parameters, you can write without '$'.
The values of columns which are not mapped in the "-- table" section will be the same with migrating table. If it is a new column, there will be null.
You can write eager or lazy as fetch value. This is important for cql queries. Lazy queries each column respectively. Unlike lazy, eager queries once and 
put the values a map.

    -- description: creates users and groups tables
        -- authoredAt: 1469630066000
        -- up:
    
        -- stage: 1
        CREATE TABLE users (
          id uuid,
          group_id uuid,
          username text,
          password text,
          score int,
          PRIMARY KEY (id)
        )
        
        -- mapping:
        -- fetch: eager
         
        -- table: backup_users->users
        username->/home/mustafa/changeName.sh $name
        score->select score from score_table where id = $id
        -- end:
        
        --table: old_table->new_table
        column_1-><path><file-name>.sh $parameter_1 constant_parameter $parameter_2 
        column_2-><sql_query>
        --end:
        
            
The Pillar command line interface expects to find migrations in conf/pillar/migrations unless overriden by the
-d command-line option.

#### Configuration

Pillar uses the [Typesafe Config][typesafeconfig] library for configuration. The Pillar command-line interface expects
to find an application.conf file in ./conf or ./src/main/resources. Given a data store called faker, the
application.conf might look like the following:

    pillar.faker {
        development {
            cassandra-seed-address: "127.0.0.1"
            cassandra-keyspace-name: "pillar_development"
        }
        test {
            cassandra-seed-address: "127.0.0.1"
            cassandra-keyspace-name: "pillar_test"
        }
        acceptance_test {
            cassandra-seed-address: ${?PILLAR_SEED_ADDRESS}
            cassandra-port: ${?PILLAR_PORT}
            cassandra-keyspace-name: "pillar_acceptance_test"
            cassandra-keyspace-name: ${?PILLAR_KEYSPACE_NAME}
            cassandra-ssl: ${?PILLAR_SSL}
            cassandra-username: ${?PILLAR_USERNAME}
            cassandra-password: ${?PILLAR_PASSWORD}
        }
    }

[typesafeconfig]:https://github.com/typesafehub/config

Notice the use of environment varaibles in the acceptance_test environment example. This is a feature of Typesafe Config
that can greatly increase the security and portability of your Pillar configuration.

#### Transport Layer Security (TLS/SSL)

Pillar will optionally enable TLS/SSL for client-to-node communications. As Pillar runs on the Java virtual machine,
normal JVM TLS/SSL configuration options apply. If the JVM executing Pillar does not already trust the certificate
presented by the Cassandra cluster, you may need to configure the trust store as documented by [Oracle][jsseref]
and [DataStax][dsssl].

Pillar does not install a custom trust manager but rather relies on the default trust manager implementation.
Configuring the default trust store requires setting two system properties, like this:

    JAVA_OPTS='-Djavax.net.ssl.trustStore=/opt/pillar/conf/truststore -Djavax.net.ssl.trustStorePassword=cassandra'

$JAVA_OPTS are passed through to the JVM when using the pillar executable.

[jsseref]:https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html
[dsssl]:https://datastax.github.io/java-driver/2.0.12/features/ssl/

#### The pillar Executable

The package installs to /opt/pillar by default. The /opt/pillar/bin/pillar executable usage looks like this:

    Usage: pillar [OPTIONS] command data-store

    OPTIONS

    -d directory
    --migrations-directory directory  The directory containing migrations

    -e env
    --environment env                 environment

    -t time
    --time-stamp time                 The migration time stamp

    PARAMETERS

    command     migrate or initialize

    data-store  The target data store, as defined in application.conf

#### Examples

Initialize the faker datastore development environment

    % pillar -e development initialize faker

Apply all migrations to the faker datastore development environment

    % pillar -e development migrate faker

### Library

You can also integrate Pillar directly into your application as a library.
Reference the acceptance spec suite for details.

### Forks

Several organizations and people have forked the Pillar code base. The most actively maintained alternative is
the [Galeria-Kaufhof fork][gkf].

[gkf]:https://github.com/Galeria-Kaufhof/pillar

### Release Notes

#### 1.0.1

* Add a "destroy" method to drop a keyspace (iamsteveholmes)

#### 1.0.3

* Clarify documentation (pvenable)
* Update DataStax Cassandra driver to version 2.0.2 (magro)
* Update Scala to version 2.10.4 (magro)
* Add cross-compilation to Scala version 2.11.1 (magro)
* Shutdown cluster in migrate & initialize (magro)
* Transition support from StreamSend to Chris O'Meara (comeara)

#### 2.0.0

* Allow configuration of Cassandra port (fkoehler)
* Rework Migrator interface to allow passing a Session object when integrating Pillar as a library (magro, comeara)

#### 2.0.1

* Update a argot dependency to version 1.0.3 (magro)

#### 2.1.0

* Update DataStax Cassandra driver to version 3.0.0 (MarcoPriebe)
* Fix documentation issue where authored_at represented as seconds rather than milliseconds (jhungerford)
* Introduce PILLAR_SEED_ADDRESS environment variable (comeara)

#### 2.1.1

* Fix deduplicate error during merge, ref. issue #32 (ilovezfs)

#### 2.2.0

* Add feature to read registry from files (sadowskik)
* Add TLS/SSL support(bradhandy, comeara)
* Add authentication support (bradhandy, comeara)

#### 2.3.0

* Add multiple stages per migration (sadowskik)
