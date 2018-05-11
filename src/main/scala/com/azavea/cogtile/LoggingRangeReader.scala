package com.azavea.cogtile

import geotrellis.util._

case class LoggingRangeReader(rr: RangeReader) extends RangeReader {
    import LoggingRangeReader._

    def totalLength: Long = rr.totalLength

    override def readRange(start: Long, length: Int): Array[Byte] = {
        println(s"RangeReader: ${humanReadableByteCount(length)} at $start")
        rr.readRange(start, length)
    }

    protected def readClippedRange(start: Long, length: Int): Array[Byte] = ???

    override def readAll(): Array[Byte] = {
        println("RangeReader: ALL")
        rr.readAll()
    }
}

object LoggingRangeReader {
    def humanReadableByteCount(bytes: Long, si: Boolean = true): String = {
        val unit = if (si) 1000 else 1024
        if (bytes < unit) return bytes + " B";
        val exp = math.log(bytes) / math.log(unit)
        val pre = (if (si) "kMGTPE" else "KMGTPE").charAt(exp.toInt - 1) + ( if (si) "" else "i" ) + "B";
        val x = (bytes / math.pow(unit, exp)).toInt
        s"$x $pre"
    }
}