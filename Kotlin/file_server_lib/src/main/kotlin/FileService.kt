package com.sz.file_server.lib

import com.sz.smart_home.common.NetworkService
import com.sz.smart_home.common.NetworkServiceConfig
import com.sz.smart_home.common.ResponseError
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder

enum class RequestId {
    Get,
    Set,
    GetLast,
    FetFileVersion
}

data class KeyValue(val key: Int, val value: ByteArray) {
    fun getBinaryLength(): Int {
        return 8 + value.size
    }

    fun toBinary(buffer: ByteBuffer) {
        buffer.putInt(key)
        buffer.putInt(value.size)
        buffer.put(value)
    }
}

data class File(val version: Int, val data: ByteArray)

data class GetResponse(val dbVersion: Int, val data: Map<Int, File>)

data class GetLastResponse(val dbVersion: Int, val data: Pair<Int, File>?)

data class GetFileVersionResponse(val dbVersion: Int, val fileVersion: Int?)

data class FileServiceConfig(val userId: Int, val key: ByteArray, val hostName: String, val port: Int, val timeoutMs: Int,
                             val dbName: String)

class FileService(config: FileServiceConfig):
    NetworkService(NetworkServiceConfig(
        prefix = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(config.userId).array(),
        key = config.key,
        hostName = config.hostName,
        port = config.port,
        timeoutMs = config.timeoutMs,
        useBzip2 = false)) {

    private val dbName = config.dbName

    fun get(key1: Int, key2: Int, callback: Callback<GetResponse>) {
        val request = buildGetRequest(RequestId.Get, key1, key2)
        send(request, object: Callback<ByteArray> {
            override fun onResponse(response: ByteArray) {
                try {
                    when (response[0]) {
                        //no error
                        0.toByte() -> callback.onResponse(decodeGetResponse(response.drop(1)))
                        // error
                        else -> throw ResponseError(response)
                    }
                } catch (t: Throwable) {
                    callback.onFailure(t)
                }
            }

            override fun onFailure(t: Throwable) {
                callback.onFailure(t)
            }
        })
    }

    fun getLast(key1: Int, key2: Int, callback: Callback<GetLastResponse>) {
        val request = buildGetRequest(RequestId.GetLast, key1, key2)
        send(request, object: Callback<ByteArray> {
            override fun onResponse(response: ByteArray) {
                try {
                    when (response[0]) {
                        //no error
                        0.toByte() -> callback.onResponse(decodeGetLastResponse(response.drop(1)))
                        // error
                        else -> throw ResponseError(response)
                    }
                } catch (t: Throwable) {
                    callback.onFailure(t)
                }
            }

            override fun onFailure(t: Throwable) {
                callback.onFailure(t)
            }
        })
    }

    fun getFileVersion(key: Int, callback: Callback<GetFileVersionResponse>) {
        val request = buildGetFileVersionRequest(key)
        send(request, object: Callback<ByteArray> {
            override fun onResponse(response: ByteArray) {
                try {
                    when (response[0]) {
                        //no error
                        0.toByte() -> callback.onResponse(decodeGetFileVersionResponse(response.drop(1)))
                        // error
                        else -> throw ResponseError(response)
                    }
                } catch (t: Throwable) {
                    callback.onFailure(t)
                }
            }

            override fun onFailure(t: Throwable) {
                callback.onFailure(t)
            }
        })
    }

    private fun decodeGetResponse(response: List<Byte>): GetResponse {
        val buffer = ByteBuffer.wrap(response.toByteArray()).order(ByteOrder.LITTLE_ENDIAN)
        val dbVersion = buffer.getInt()
        var length = buffer.getInt()
        val result = mutableMapOf<Int, File>()
        while (length-- > 0) {
            val fileVersion = buffer.getInt()
            val key = buffer.getInt()
            val valueLength = buffer.getInt()
            val value = ByteArray(valueLength)
            buffer.get(value)
            result[key] = File(fileVersion, value)
        }
        if (buffer.hasRemaining()) {
            throw IOException("Incorrect response length")
        }
        return GetResponse(dbVersion, result)
    }

    private fun decodeGetLastResponse(response: List<Byte>): GetLastResponse {
        val buffer = ByteBuffer.wrap(response.toByteArray()).order(ByteOrder.LITTLE_ENDIAN)
        val dbVersion = buffer.getInt()
        val fileIsPresent = buffer.get() != 0.toByte()
        var kv: Pair<Int, File>? = null
        if (fileIsPresent) {
            val fileVersion = buffer.getInt()
            val key = buffer.getInt()
            val valueLength = buffer.getInt()
            val value = ByteArray(valueLength)
            buffer.get(value)
            kv = Pair(key, File(fileVersion, value))
        }
        if (buffer.hasRemaining()) {
            throw IOException("Incorrect response length")
        }
        return GetLastResponse(dbVersion, kv)
    }

    private fun decodeGetFileVersionResponse(response: List<Byte>): GetFileVersionResponse {
        if (response.size != 8) {
            throw IOException("Incorrect response length")
        }
        val buffer = ByteBuffer.wrap(response.toByteArray()).order(ByteOrder.LITTLE_ENDIAN)
        val dbVersion = buffer.getInt()
        val fileVersion = buffer.getInt()
        return GetFileVersionResponse(dbVersion, if (fileVersion == 0) { null } else { fileVersion})
    }

    private fun buildGetRequest(id: RequestId, key1: Int, key2: Int): ByteArray {
        val bytes = dbName.toByteArray(Charsets.UTF_8)
        val buffer = ByteBuffer.allocate(9 + bytes.size + 1).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(id.ordinal.toByte())
        buffer.put(bytes.size.toByte())
        buffer.put(bytes)
        buffer.putInt(key1)
        buffer.putInt(key2)
        return buffer.array()
    }

    private fun buildGetFileVersionRequest(key: Int): ByteArray {
        val bytes = dbName.toByteArray(Charsets.UTF_8)
        val buffer = ByteBuffer.allocate(5 + bytes.size + 1).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(RequestId.FetFileVersion.ordinal.toByte())
        buffer.put(bytes.size.toByte())
        buffer.put(bytes)
        buffer.putInt(key)
        return buffer.array()
    }

    fun set(dbVersion: Int, values: List<KeyValue>, callback: Callback<Unit>) {
        val request = buildSetRequest(dbVersion, values)
        val response = send(request, object: Callback<ByteArray> {
            override fun onResponse(response: ByteArray) {
                try {
                    if (response[0] != 0.toByte()) {
                        throw ResponseError(response)
                    }
                    callback.onResponse(Unit)
                } catch (t: Throwable) {
                    callback.onFailure(t)
                }
            }

            override fun onFailure(t: Throwable) {
                callback.onFailure(t)
            }

        })
    }

    private fun buildSetRequest(dbVersion: Int, values: List<KeyValue>): ByteArray {
        val bytes = dbName.toByteArray(Charsets.UTF_8)
        val data = toBinary(values)
        val buffer = ByteBuffer.allocate(data.size + bytes.size + 6).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(RequestId.Set.ordinal.toByte())
        buffer.put(bytes.size.toByte())
        buffer.put(bytes)
        buffer.putInt(dbVersion)
        buffer.put(data)
        return buffer.array()
    }

    private fun toBinary(values: List<KeyValue>): ByteArray {
        val l = values.sumOf { it.getBinaryLength() } + 4
        val data = ByteBuffer.allocate(l).order(ByteOrder.LITTLE_ENDIAN)
        data.putInt(values.size)
        for (value in values) {
            value.toBinary(data)
        }
        return data.array()
    }
}
