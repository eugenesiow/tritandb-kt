package com.tritandb.engine.server

import com.natpryce.konfig.ConfigurationProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.overriding
import mu.KLoggable
import java.io.File

/**
 * TritanDb
 * Created by eugene on 04/07/2017.
 */
class StartServer {
    companion object: Any(), KLoggable {
        override val logger = logger()
        @JvmStatic fun main(args: Array<String>) {
            val config = ConfigurationProperties.systemProperties() overriding
                    EnvironmentVariables() overriding
                    ConfigurationProperties.fromFile(File("config/config.properties")) overriding
                    ConfigurationProperties.fromResource("defaults.properties")
            val zServer: ZmqServer = ZmqServer(config)
            logger.info("starting server...")
            zServer.start()
        }
    }
}