package com.sz.file_server.client

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.TimeSource

fun usage() {
    println("Usage: java -jar file_server_client.jar keyFileName hostName port db_name [get key1 key2][set file_names]")
}

fun main(args: Array<String>) {
    if (args.size < 6) {
        usage()
        return
    }
    val keyBytes = Files.readAllBytes(Paths.get(args[0]))
    val hostName = args[1]
    val port = args[2].toInt()
    val dbName = args[3]
    val command = args[4]

    val service = FileService(keyBytes, hostName, port, dbName)

    when (command) {
        "get" -> runGetCommand(service, args.drop(5))
        "set" -> runSetCommand(service, args.drop(5))
        else -> usage()
    }
}

fun runGetCommand(service: FileService, args: List<String>) {
    if (args.size != 2) {
        usage()
        return
    }
    val key1 = args[0].toInt()
    val key2 = args[1].toInt()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    val response = service.get(key1, key2)
    val mark2 = timeSource.markNow()
    println("Response time: ${mark2 - mark1}, db version: ${response.dbVersion}, number of files: ${response.data.size}.")
    for ((k,v) in response.data) {
        val fileName = k.toString()
        println("Writing $fileName...")
        File(k.toString()).writeBytes(v)
    }
}

fun runSetCommand(service: FileService, args: List<String>) {
    if (args.size < 2) {
        usage()
        return
    }
    val dbVersion = args[0].toInt()
    val data = args.drop(1).map { KeyValue(it.toInt(), Files.readAllBytes(Paths.get(it))) }.toList()
    service.set(dbVersion, data)
}
