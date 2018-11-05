package com.kaszub.parso

import java.io.InputStream

case class SasPageReader(inputStream: InputStream) extends InputStream {

  override def read(): Int = inputStream.read()

  override def read(b: Array[Byte]): Int = inputStream.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = inputStream.read(b, off, len)

  override def skip(n: Long): Long = inputStream.skip(n)

  override def available(): Int = inputStream.available()

  override def close(): Unit = inputStream.close()

  override def mark(readlimit: Int): Unit = inputStream.mark(readlimit)

  override def reset(): Unit = inputStream.reset()

  override def markSupported(): Boolean = inputStream.markSupported()
}
