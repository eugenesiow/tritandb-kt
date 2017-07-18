package com.tritandb.engine.server

import com.natpryce.konfig.ConfigurationProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.overriding
import java.io.File

/**
 * TritanDb
 * Created by eugene on 18/07/2017.
 */
class StartReqRepServer {
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val config = ConfigurationProperties.systemProperties() overriding
                    EnvironmentVariables() overriding
                    ConfigurationProperties.fromFile(File("config/config.properties")) overriding
                    ConfigurationProperties.fromResource("defaults.properties")
            val zServer: ZmqReqRepServer = ZmqReqRepServer(config)
            zServer.start()
        }
    }
}