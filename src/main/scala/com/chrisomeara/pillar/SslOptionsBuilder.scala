package com.chrisomeara.pillar

import java.io.FileInputStream
import java.security.{SecureRandom, KeyStore}
import javax.net.ssl.{KeyManager, TrustManagerFactory, SSLContext}

import com.datastax.driver.core.SSLOptions

class SslOptionsBuilder {
  var keystore: KeyStore = null
  var trustManagerFactory: TrustManagerFactory = null
  var sslContext: SSLContext = null

  def withKeyStore(storeFile: String, storePassword: String, storeType: String = "JKS") = {
    keystore = KeyStore.getInstance(storeType)
    keystore.load(new FileInputStream(storeFile), storePassword.toCharArray)
  }

  def withTrustManager(trustManagerAlgorithm: String = TrustManagerFactory.getDefaultAlgorithm) = {
    trustManagerFactory = TrustManagerFactory.getInstance(trustManagerAlgorithm)
  }

  def withSslContext(sslContextType: String = "SSL") = {
    sslContext = SSLContext.getInstance(sslContextType)
  }

  def build(): SSLOptions = {
    trustManagerFactory.init(keystore)
    sslContext.init(new Array[KeyManager](0), trustManagerFactory.getTrustManagers, new SecureRandom())

    new SSLOptions(sslContext, SSLOptions.DEFAULT_SSL_CIPHER_SUITES);
  }
}
