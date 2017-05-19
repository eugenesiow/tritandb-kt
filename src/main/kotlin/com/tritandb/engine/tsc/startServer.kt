package com.tritandb.engine.tsc

import com.natpryce.konfig.ConfigurationProperties
import com.natpryce.konfig.ConfigurationProperties.Companion.systemProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.overriding
import com.tritandb.engine.server.ZmqServer
import java.io.File


/**
* TritanDb
* Created by eugene on 12/05/2017.
*/

fun main(args: Array<String>) {
    val config = systemProperties() overriding
            EnvironmentVariables() overriding
            ConfigurationProperties.fromFile(File("config/config.properties")) overriding
            ConfigurationProperties.fromResource("defaults.properties")
    val zServer: ZmqServer = ZmqServer(config)
    zServer.start()
}
