package com.chrisomeara.pillar.cli

import java.io.{FileReader, File}
import java.util.Properties

import com.chrisomeara.pillar._
import com.datastax.driver.core.Cluster
import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}

object App {
  def apply(reporter: Reporter = new PrintStreamReporter(System.out)): App = {
    new App(reporter)
  }

  def main(arguments: Array[String]) {
    try {
      App().run(arguments)
    } catch {
      case exception: Exception =>
        System.err.println(exception.getMessage)
        System.exit(1)
    }

    System.exit(0)
  }
}

class App(reporter: Reporter) {
  def run(arguments: Array[String]) {
    val commandLineConfiguration = CommandLineConfiguration.buildFromArguments(arguments)
    val registry = Registry.fromDirectory(new File(commandLineConfiguration.migrationsDirectory, commandLineConfiguration.dataStore))
    val configuration = ConfigFactory.load()
    val dataStoreName = commandLineConfiguration.dataStore
    val environment = commandLineConfiguration.environment
    val keyspace = getFromConfiguration(configuration, dataStoreName, environment, "cassandra-keyspace-name")
    val seedAddress = getFromConfiguration(configuration, dataStoreName, environment, "cassandra-seed-address")
    val port = Integer.valueOf(getFromConfiguration(configuration, dataStoreName, environment, "cassandra-port", Some(9042.toString)))
    val properties = loadProperties(configuration, dataStoreName, environment)

    val clusterBuilder = Cluster.builder().addContactPoint(seedAddress).withPort(port)
    if (properties != null) {
      val authProvider = PlainTextAuthProviderFactory.fromProperties(properties)
      if (authProvider != null) {
        clusterBuilder.withAuthProvider(authProvider)
      }

      val useSsl = properties.getProperty("useSsl")
      if (!Strings.isNullOrEmpty(useSsl) && useSsl.toBoolean) {
        val optionsBuilder = new SslOptionsBuilder

        val trustStoreFile = properties.getProperty("trust-store-path")
        val trustStorePassword = properties.getProperty("trust-store-password")
        if (!Strings.isNullOrEmpty(trustStoreFile) && !Strings.isNullOrEmpty(trustStorePassword)) {
          optionsBuilder.withKeyStore(trustStoreFile, trustStorePassword)
        }

        optionsBuilder.withSslContext()
        optionsBuilder.withTrustManager()

        clusterBuilder.withSSL(optionsBuilder.build())
      }
    }

    val cluster = clusterBuilder.build()
    val session = commandLineConfiguration.command match {
      case Initialize => cluster.connect()
      case _ => cluster.connect(keyspace)
    }
    val command = Command(commandLineConfiguration.command, session, keyspace, commandLineConfiguration.timeStampOption, registry)

    try {
      CommandExecutor().execute(command, reporter)
    } finally {
      session.close()
    }
  }

  private def getFromConfiguration(configuration: Config, name: String, environment: String, key: String, default: Option[String] = None): String = {
    val path = s"pillar.$name.$environment.$key"
    if (configuration.hasPath(path)) return configuration.getString(path)
    if (default.eq(None)) throw new ConfigurationException(s"$path not found in application configuration")
    default.get
  }

  private def loadProperties(configuration: Config, name: String, environment: String): Properties = {
    val properties = new Properties()
    val credentialsPath = getFromConfiguration(configuration, name, environment, "credentials-path")

    if (Strings.isNullOrEmpty(credentialsPath)) {
      return null
    }

    properties.load(new FileReader(credentialsPath))
    properties
  }

}