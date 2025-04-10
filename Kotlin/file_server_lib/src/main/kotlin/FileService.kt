package com.sz.file_server.lib

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import com.sz.smart_home.common.NetworkService
import com.sz.smart_home.common.ResponseError
import org.apache.commons.io.output.ByteArrayOutputStream
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

class FileService(key: ByteArray, hostName: String, port: Int, private val dbName: String): NetworkService(key, hostName, port) {
    fun get(key1: Int, key2: Int): GetResponse {
        val request = buildGetRequest(RequestId.Get, key1, key2)
        val response = send(request)
        return when (response[0]) {
            //no error
            0.toByte() -> decodeGetResponse(response.drop(1))
            // error
            else -> throw ResponseError(response)
        }
    }

    fun getLast(key1: Int, key2: Int): GetLastResponse {
        val request = buildGetRequest(RequestId.GetLast, key1, key2)
        val response = send(request)
        return when (response[0]) {
            //no error
            0.toByte() -> decodeGetLastResponse(response.drop(1))
            // error
            else -> throw ResponseError(response)
        }
    }

    fun getFileVersion(key: Int): GetFileVersionResponse {
        val request = buildGetFileVersionRequest(key)
        val response = send(request)
        return when (response[0]) {
            //no error
            0.toByte() -> decodeGetFileVersionResponse(response.drop(1))
            // error
            else -> throw ResponseError(response)
        }
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

    fun set(dbVersion: Int, values: List<KeyValue>) {
        val request = buildSetRequest(dbVersion, values)
        val response = send(request)
        if (response[0] != 0.toByte()) {
            throw ResponseError(response)
        }
    }

    private fun buildSetRequest(dbVersion: Int, values: List<KeyValue>): ByteArray {
        val bytes = dbName.toByteArray(Charsets.UTF_8)
        val compressed = compress(values)
        val buffer = ByteBuffer.allocate(compressed.size + bytes.size + 6).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(RequestId.Set.ordinal.toByte())
        buffer.put(bytes.size.toByte())
        buffer.put(bytes)
        buffer.putInt(dbVersion)
        buffer.put(compressed)
        return buffer.array()
    }

    private fun compress(values: List<KeyValue>): ByteArray {
        val l = values.sumOf { it.getBinaryLength() } + 4
        val uncompressed = ByteBuffer.allocate(l).order(ByteOrder.LITTLE_ENDIAN)
        uncompressed.putInt(values.size)
        for (value in values) {
            value.toBinary(uncompressed)
        }
        val stream = ByteArrayOutputStream()
        val outStream = BZip2CompressorOutputStream(stream, 9)
        outStream.write(uncompressed.array())
        outStream.close()
        return stream.toByteArray()
    }
}
