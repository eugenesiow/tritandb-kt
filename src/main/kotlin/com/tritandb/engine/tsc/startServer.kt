package com.tritandb.engine.tsc

import com.natpryce.konfig.ConfigurationProperties.Companion.systemProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.overriding
import com.tritandb.engine.server.ZmqServer


/**
* TritanDb
* Created by eugene on 12/05/2017.
*/

fun main(args: Array<String>) {
    val config = systemProperties() overriding
            EnvironmentVariables()
    val zServer: ZmqServer = ZmqServer(config)
    zServer.start()
}
