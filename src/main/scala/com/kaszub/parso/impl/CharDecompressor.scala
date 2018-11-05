package com.kaszub.parso.impl

import com.typesafe.scalalogging.Logger

/**
* Implementation of the CHAR compression algorithm which corresponds to the literal "SASYZCRL".
* Decompresses using the Run Length Encoding algorithm. Refer the documentation for further details.
* It follows the general contract provided by the interface <code>Decompressor</code>.
*
*/
object CharDecompressor extends Decompressor {
  /**
    * Object for writing logs.
    */
  private val logger = Logger[CharDecompressor.type]

  /**
    * The function to decompress data. Compressed data are an array of bytes with control bytes and data bytes.
    * The project documentation contains descriptions of the decompression algorithm.
    *
    * @param offset       the offset of bytes array in {@link SasFileParser#cachedPage} that contains compressed data.
    * @param length       the length of bytes array that contains compressed data.
    * @param resultLength the length of bytes array that contains decompressed data.
    * @param page         an array of bytes with compressed data.
    * @return an array of bytes with decompressed data.
    */
  @Override
  def decompressRow(offset: Int, length: Int, resultLength: Int, page: Seq[Byte]): Seq[Byte] = {

    var currentByteIndex = 0
    var newResultByteArray = Seq[Byte]()

    while (currentByteIndex < length) {
      val controlByte = page(offset + currentByteIndex) & 0xF0
      val endOfFirstByte = page(offset + currentByteIndex) & 0x0F

      controlByte match {
        case 0x30 | 0x20 | 0x10 | 0x00 =>
          if (currentByteIndex != length - 1) {
            val countOfBytesToCopy = (page(offset + currentByteIndex + 1) & 0xFF) + 64 +
              page(offset + currentByteIndex) * 256
            newResultByteArray = newResultByteArray ++ page.slice(offset + currentByteIndex + 2, countOfBytesToCopy + offset + currentByteIndex + 2)
            currentByteIndex += countOfBytesToCopy + 1
          }
        case 0x40 =>
          val copyCounter = endOfFirstByte * 16 + (page(offset + currentByteIndex + 1) & 0xFF)
          newResultByteArray = newResultByteArray ++ (0 until copyCounter + 18).
            map(_ => page(offset + currentByteIndex + 2))
          currentByteIndex += 2
        case 0x50 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte * 256 + (page(offset + currentByteIndex + 1) & 0xFF) + 17).
            map(_ => 0x40)
          currentByteIndex += 1
        case 0x60 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte * 256 + (page(offset + currentByteIndex + 1) & 0xFF) + 17).
            map(_ => 0x20)
          currentByteIndex += 1
        case 0x70 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte * 256 + (page(offset + currentByteIndex + 1) & 0xFF) + 17).
            map(_ => 0x00)
          currentByteIndex += 1
        case 0x80 | 0x90 | 0xA0 | 0xB0 =>
          val countOfBytesToCopy = Math.min(endOfFirstByte + 1 + (controlByte - 0x80),
            length - (currentByteIndex + 1))
          newResultByteArray = newResultByteArray ++ page.slice(offset + currentByteIndex + 1, countOfBytesToCopy + offset + currentByteIndex + 1)
          currentByteIndex += countOfBytesToCopy
        case 0xC0 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte + 3).map(_ => page(offset + currentByteIndex + 1))
          currentByteIndex += 1
        case 0xD0 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte + 2).map(_ => 0x40)
        case 0xE0 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte + 2).map(_ => 0x20)
        case 0xF0 =>
          newResultByteArray = newResultByteArray ++ (0 until endOfFirstByte + 2).map(_ => 0x00)
        case _ =>
          logger.error("Error control byte: {}", controlByte)
      }
      currentByteIndex+=1
    }

    newResultByteArray
  }
}
