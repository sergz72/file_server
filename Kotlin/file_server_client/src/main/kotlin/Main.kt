package com.sz.file_server.client

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.TimeSource

fun usage() {
    println("Usage: java -jar file_server_client.jar keyFileName hostName port db_name [get key1 key2][get_last key1 key2][get_version key][set file_names]")
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
        "get_last" -> runGetLastCommand(service, args.drop(5))
        "get_version" -> runGetFileVersionCommand(service, args.drop(5))
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
        println("File $fileName version ${v.version}.")
        File(fileName).writeBytes(v.data)
    }
}

fun runGetLastCommand(service: FileService, args: List<String>) {
    if (args.size != 2) {
        usage()
        return
    }
    val key1 = args[0].toInt()
    val key2 = args[1].toInt()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    val response = service.getLast(key1, key2)
    val mark2 = timeSource.markNow()
    val cnt = if (response.data == null) { 0 } else { 1 }
    println("Response time: ${mark2 - mark1}, db version: ${response.dbVersion}, number of files: $cnt.")
    if (response.data != null) {
        val fileName = response.data.first.toString()
        println("File $fileName version ${response.data.second.version}.")
        File(fileName).writeBytes(response.data.second.data)
    }
}

fun runGetFileVersionCommand(service: FileService, args: List<String>) {
    if (args.size != 1) {
        usage()
        return
    }
    val key = args[0].toInt()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    val response = service.getFileVersion(key)
    val mark2 = timeSource.markNow()
    val version = if (response.fileVersion == null) {
        "none"
    } else {
        response.fileVersion.toString()
    }
    println("Response time: ${mark2 - mark1}, db version: ${response.dbVersion}, file version: $version.")
}

fun runSetCommand(service: FileService, args: List<String>) {
    if (args.size < 2) {
        usage()
        return
    }
    val dbVersion = args[0].toInt()
    val data = args.drop(1).map { KeyValue(it.toInt(), Files.readAllBytes(Paths.get(it))) }.toList()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    service.set(dbVersion, data)
    val mark2 = timeSource.markNow()
    println("Response time: ${mark2 - mark1}.")
}
