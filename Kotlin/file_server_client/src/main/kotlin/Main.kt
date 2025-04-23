package com.sz.file_server.client

import com.sz.file_server.lib.*
import com.sz.smart_home.common.NetworkService
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.TimeSource

fun usage() {
    println("Usage: java -jar file_server_client.jar userId keyFileName hostName port db_name [get key1 key2][get_last key1 key2][get_version key][set file_names]")
}

suspend fun main(args: Array<String>) {
    if (args.size < 6) {
        usage()
        return
    }
    val userId = args[0].toInt()
    val keyBytes = Files.readAllBytes(Paths.get(args[1]))
    val hostName = args[2]
    val port = args[3].toInt()
    val dbName = args[4]
    val command = args[5]

    val config = FileServiceConfig(
        userId = userId,
        key = keyBytes,
        hostName = hostName,
        port = port,
        dbName = dbName,
        timeoutMs = 1000
    )
    val service = FileService(config)

    val channel = Channel<Unit>()

    when (command) {
        "get" -> runGetCommand(service, args.drop(6), channel)
        "get_last" -> runGetLastCommand(service, args.drop(6), channel)
        "get_version" -> runGetFileVersionCommand(service, args.drop(6), channel)
        "set" -> runSetCommand(service, args.drop(6), channel)
        else -> usage()
    }
}

@OptIn(DelicateCoroutinesApi::class)
suspend fun runGetCommand(service: FileService, args: List<String>, channel: Channel<Unit>) {
    if (args.size != 2) {
        usage()
        return
    }
    val key1 = args[0].toInt()
    val key2 = args[1].toInt()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    service.get(key1, key2, object: NetworkService.Callback<GetResponse> {
        override fun onResponse(response: GetResponse) {
            val mark2 = timeSource.markNow()
            println("Response time: ${mark2 - mark1}, db version: ${response.dbVersion}, number of files: ${response.data.size}.")
            for ((k,v) in response.data) {
                val fileName = k.toString()
                println("File $fileName version ${v.version}.")
                File(fileName).writeBytes(v.data)
            }
            GlobalScope.launch { channel.send(Unit) }
        }

        override fun onFailure(t: Throwable) {
            println(t.message)
            GlobalScope.launch { channel.send(Unit) }
        }
    })
    channel.receive()
}

@OptIn(DelicateCoroutinesApi::class)
suspend fun runGetLastCommand(service: FileService, args: List<String>, channel: Channel<Unit>) {
    if (args.size != 2) {
        usage()
        return
    }
    val key1 = args[0].toInt()
    val key2 = args[1].toInt()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    service.getLast(key1, key2, object: NetworkService.Callback<GetLastResponse> {
        override fun onResponse(response: GetLastResponse) {
            val mark2 = timeSource.markNow()
            val cnt = if (response.data == null) { 0 } else { 1 }
            println("Response time: ${mark2 - mark1}, db version: ${response.dbVersion}, number of files: $cnt.")
            if (response.data != null) {
                val fileName = response.data!!.first.toString()
                println("File $fileName version ${response.data!!.second.version}.")
                File(fileName).writeBytes(response.data!!.second.data)
            }
            GlobalScope.launch { channel.send(Unit) }
        }

        override fun onFailure(t: Throwable) {
            println(t.message)
            GlobalScope.launch { channel.send(Unit) }
        }
    })
    channel.receive()
}

@OptIn(DelicateCoroutinesApi::class)
suspend fun runGetFileVersionCommand(service: FileService, args: List<String>, channel: Channel<Unit>) {
    if (args.size != 1) {
        usage()
        return
    }
    val key = args[0].toInt()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    service.getFileVersion(key, object: NetworkService.Callback<GetFileVersionResponse> {
        override fun onResponse(response: GetFileVersionResponse) {
            val mark2 = timeSource.markNow()
            val version = if (response.fileVersion == null) {
                "none"
            } else {
                response.fileVersion.toString()
            }
            println("Response time: ${mark2 - mark1}, db version: ${response.dbVersion}, file version: $version.")
            GlobalScope.launch { channel.send(Unit) }
        }

        override fun onFailure(t: Throwable) {
            println(t.message)
            GlobalScope.launch { channel.send(Unit) }
        }
    })
    channel.receive()
}

@OptIn(DelicateCoroutinesApi::class)
suspend fun runSetCommand(service: FileService, args: List<String>, channel: Channel<Unit>) {
    if (args.size < 2) {
        usage()
        return
    }
    val dbVersion = args[0].toInt()
    val data = args.drop(1).map { KeyValue(it.toInt(), Files.readAllBytes(Paths.get(it))) }.toList()
    val timeSource = TimeSource.Monotonic
    val mark1 = timeSource.markNow()
    service.set(dbVersion, data, object: NetworkService.Callback<Unit> {
        override fun onResponse(response: Unit) {
            val mark2 = timeSource.markNow()
            println("Response time: ${mark2 - mark1}.")
            GlobalScope.launch { channel.send(Unit) }
        }

        override fun onFailure(t: Throwable) {
            println(t.message)
            GlobalScope.launch { channel.send(Unit) }
        }
    })
    channel.receive()
}
