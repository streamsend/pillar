package com.chrisomeara.pillar

import java.io.FileReader
import java.util.Properties

import com.datastax.driver.core.PlainTextAuthProvider
import com.google.common.base.Strings

object PlainTextAuthProviderFactory {
  def fromProperties(properties: Properties): PlainTextAuthProvider = {
    val username = properties.getProperty("username")
    val password = properties.getProperty("password")
    if (Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) {
      return null
    }

    new PlainTextAuthProvider(username, password)
  }
}
