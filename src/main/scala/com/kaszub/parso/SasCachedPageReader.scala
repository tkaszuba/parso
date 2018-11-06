package com.kaszub.parso

import java.io.{IOException, InputStream}

import com.kaszub.parso.impl.SasFileParser.EmptyInputStream

import scala.util.{Success, Try}

class SasCachedPageReader(input: InputStream) extends SasPageReader(input) {

  private val EmptyCache = Seq[Byte]()
  private var cachedPage = EmptyCache

  override def isUsingCache: Boolean = cachedPage != EmptyCache

  override def cachePage(properties: SasFileProperties): Try[Long] = {
    cachedPage = EmptyCache
    val res = readPage(properties, false)
    cachedPage = res.readBytes.head
    Success(res.relPosition.get)
  }

  /**
    * The function to read the list of bytes arrays from the sas7bdat file. The array of offsets and the array of
    * lengths serve as input data that define the location and number of bytes the function must read.
    *
    * @param offset the array of offsets.
    * @param length the array of lengths.
    * @return the list of bytes arrays.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  private def readBytes(page: Seq[Byte], offset: Seq[Long], length: Seq[Int]): Seq[Seq[Byte]] = {
    offset.zipWithIndex.map(e => {
      val curOffset = e._1.toInt
      if (page.length < curOffset) throw new IOException(EmptyInputStream)
      page.slice(curOffset, curOffset + length(e._2))
    })
  }

  /**
    * The function to read the list of bytes arrays from the sas7bdat file, if a cache is available it will use
    * the cache.
    * NOTE: if reading from cache the absolute and relative position are not available.
    *
    * @param position the position to start reading from. If reading from the cache the position is always 0,
    *                 ie: the beginning of the cached page
    * @param offset   the array of offsets.
    * @param length   the array of lengths.
    * @param rewind   rewind the input stream to the same location as before the read
    * @return the list of bytes arrays.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  override def readBytes(position: Long, offset: Seq[Long], length: Seq[Int], rewind: Boolean): BytesReadResult = {
    if (!isUsingCache)
      super.readBytes(position, offset, length, rewind)
    else
      BytesReadResult(false, None, None, readBytes(cachedPage, offset, length))
  }

  override def moveToEndOfHeader(properties: SasFileProperties, res: BytesReadResult): Try[Long] = {
    super.moveToEndOfHeader(properties, res)
    cachePage(properties)
  }

}

object SasCachedPageReader {
  def apply(inputStream: InputStream): SasPageReader = new SasCachedPageReader(inputStream)
}