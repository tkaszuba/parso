package com.kaszub.parso

import java.io.{EOFException, IOException, InputStream}

import com.kaszub.parso.impl.SasFileParser.EmptyInputStream

import scala.util.{Failure, Success, Try}

/**
  * The result of reading bytes from a file
  *
  * @param eof         whether the eof file was reached while reading the stream
  * @param absPosition the absolute position when reading the input stream
  * @param relPosition the last relative position when reading the input stream
  * @param readBytes   the matrix of read bytes
  */
case class BytesReadResult(eof: Boolean = false,
                           absPosition: Option[Long] = None,
                           relPosition: Option[Long] = None, readBytes: Seq[Seq[Byte]] = Seq())

class SasPageReader(private val inputStream: InputStream) extends InputStream {

  override def read(): Int = inputStream.read()

  override def read(b: Array[Byte]): Int = inputStream.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = inputStream.read(b, off, len)

  /**
    * The function to read the list of bytes arrays from the sas7bdat file. The array of offsets and the array of
    * lengths serve as input data that define the location and number of bytes the function must read.
    *
    * @param offset the array of offsets.
    * @param length the array of lengths.
    * @param rewind rewind the input stream to the same location as before the read
    * @return the list of bytes arrays.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def readBytes(position: Long, offset: Seq[Long], length: Seq[Int], rewind: Boolean = true): BytesReadResult = {

    if (rewind)
      mark(Int.MaxValue)

    val res = (0L until offset.length).foldLeft(BytesReadResult(relPosition = Some(position)))((acc: BytesReadResult, i: Long) => {
      skipTo(offset(i.toInt) - acc.relPosition.get).get
      // println(acc.lastPosition)
      val temp = new Array[Byte](length(i.toInt))

      val eof = Try(read(temp, 0, length(i.toInt))) match {
        case Success(_) => false
        case Failure(e) => e match {
          case _: EOFException => true
          case _ => false
        }
      }

      val lastRelPosition = offset(i.toInt) + length(i.toInt).toLong

      BytesReadResult(
        eof.asInstanceOf[Boolean], //todo: remove that asInstanceOf, required since intelij has some parsing bug
        Some(acc.relPosition.get + lastRelPosition),
        Some(lastRelPosition),
        acc.readBytes :+ temp.toSeq
      )
    })

    if (rewind)
      reset()

    res
  }

  def readPage(properties: SasFileProperties, rewind: Boolean = false): BytesReadResult =
    readBytes(0L, Seq(0L), Seq(properties.pageLength), rewind)

  override def skip(n: Long): Long = inputStream.skip(n)

  def moveToEndOfHeader(properties: SasFileProperties, res: BytesReadResult): Try[Long] =
    skipTo(properties.headerLength - res.relPosition.get)

  protected def skipTo(skipTo: Long): Try[Long] = {
    Success((0L to skipTo).foldLeft(0L)((actuallySkipped, j) => {
      //println(actuallySkipped)
      if (actuallySkipped >= skipTo) return Success(actuallySkipped)
      try
        actuallySkipped + inputStream.skip(skipTo - actuallySkipped)
      catch {
        case e: IOException => return Failure(throw new IOException(EmptyInputStream))
        case e: Throwable => return Failure(e)
      }
    }))
  }

  def isUsingCache: Boolean = false

  def cachePage(properties: SasFileProperties): Try[Long] = skipTo(properties.pageLength)

  override def available(): Int = inputStream.available()

  override def close(): Unit = inputStream.close()

  override def mark(readLimit: Int): Unit = inputStream.mark(readLimit)

  override def reset(): Unit = inputStream.reset()

  override def markSupported(): Boolean = inputStream.markSupported()
}

object SasPageReader {
  def apply(inputStream: InputStream): SasPageReader = new SasPageReader(inputStream)
}
