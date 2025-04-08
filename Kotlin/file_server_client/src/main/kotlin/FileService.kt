package com.sz.file_server.client

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import com.sz.smart_home.common.NetworkService
import com.sz.smart_home.common.ResponseError
import org.apache.commons.io.output.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder

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

class FileService(key: ByteArray, hostName: String, port: Int, val dbName: String): NetworkService(key, hostName, port) {
    fun get(key1: Int, key2: Int): Map<Int, ByteArray> {
        val request = buildGetRequest(key1, key2)
        val response = send(request)
        return when (response[0]) {
            //no error
            0.toByte() -> decodeGetResponse(response.drop(1))
            // error
            else -> throw ResponseError(response)
        }
    }

    private fun decodeGetResponse(response: List<Byte>): Map<Int, ByteArray> {
        val buffer = ByteBuffer.wrap(response.toByteArray()).order(ByteOrder.LITTLE_ENDIAN)
        var length = buffer.getInt()
        val result = mutableMapOf<Int, ByteArray>()
        while (length-- > 0) {
            val key = buffer.getInt()
            val valueLength = buffer.getInt()
            val value = ByteArray(valueLength)
            buffer.get(value)
            result[key] = value
        }
        if (buffer.hasRemaining()) {
            throw IOException("Incorrect response length")
        }
        return result
    }

    private fun buildGetRequest(key1: Int, key2: Int): ByteArray {
        val bytes = dbName.toByteArray(Charsets.UTF_8)
        val buffer = ByteBuffer.allocate(9 + bytes.size + 1).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(0)
        buffer.put(bytes.size.toByte())
        buffer.put(bytes)
        buffer.putInt(key1)
        buffer.putInt(key2)
        return buffer.array()
    }

    fun set(values: List<KeyValue>) {
        val request = buildSetRequest(values)
        val response = send(request)
        if (response[0] != 0.toByte()) {
            throw ResponseError(response)
        }
    }

    private fun buildSetRequest(values: List<KeyValue>): ByteArray {
        val bytes = dbName.toByteArray(Charsets.UTF_8)
        val compressed = compress(values)
        val buffer = ByteBuffer.allocate(compressed.size + bytes.size + 2).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(1)
        buffer.put(bytes.size.toByte())
        buffer.put(bytes)
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