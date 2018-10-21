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
    * Unambiguous class instance.
    */
  //val INSTANCE = classOf[this.type]
  /**
    * Object for writing logs.
    */
  private val logger = Logger[this.type]

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

    var currentResultArrayIndex = 0
    var currentByteIndex = 0

    for (i <- 0 to length) {
      val controlByte = page(offset + currentByteIndex) & 0xF0
      val endOfFirstByte = page(offset + currentByteIndex) & 0x0

      controlByte match {
        case 0x30 | 0x20 | 0x10 | 0x00 => {
          if (currentByteIndex != length - 1) {
            val countOfBytesToCopy = (page(offset + currentByteIndex + 1) & 0xFF) + 64 +
              page(offset + currentByteIndex) * 256
            //System.arraycopy(page, offset + currentByteIndex + 2, resultByteArray, currentResultArrayIndex, countOfBytesToCopy)
            currentByteIndex += countOfBytesToCopy + 1
            currentResultArrayIndex += countOfBytesToCopy
          }
        }
        case 0x40 => {
          val copyCounter = endOfFirstByte * 16 + (page(offset + currentByteIndex + 1) & 0xFF)
          //for (i <- 0 to copyCounter)
          //  resultByteArray(currentResultArrayIndex += 1) = page(offset + currentByteIndex + 2)
          //}
          currentByteIndex += 2
        }
        case 0x50 => {
          //for (i <- 0 to endOfFirstByte * 256 + (page(offset + currentByteIndex + 1) & 0xFF) + 17)
          //  resultByteArray(currentResultArrayIndex += 1) = 0x40
          currentByteIndex += 1
        }
        case 0x60 => {
          //for (i <- 0 to endOfFirstByte * 256 + (page(offset + currentByteIndex + 1) & 0xFF) + 17)
          //  resultByteArray(currentResultArrayIndex += 1) = 0x20
          currentByteIndex += 1
        }
        case 0x70 => {
          //for (i <- 0 to endOfFirstByte * 256 + (page(offset + currentByteIndex + 1) & 0xFF) + 17)
          //  resultByteArray(currentResultArrayIndex += 1) = 0x00
          currentByteIndex += 1
        }
        case 0x80 | 0x90 | 0xA0 | 0xB0 => {
          var countOfBytesToCopy = Math.min(endOfFirstByte + 1 + (controlByte - 0x80),
            length - (currentByteIndex + 1))
          //System.arraycopy(page, offset + currentByteIndex + 1, resultByteArray,
          //  currentResultArrayIndex, countOfBytesToCopy);
          currentByteIndex += countOfBytesToCopy
          currentResultArrayIndex += countOfBytesToCopy
        }
        case 0xC0 => {
          //for (i <- 0 to endOfFirstByte + 3)
          //  resultByteArray(currentResultArrayIndex += 1) =  page(offset + currentByteIndex + 1)
          currentByteIndex += 1
        }
        case 0xD0 => {
          //for (i <- 0 to endOfFirstByte + 2)
          //  resultByteArray(currentResultArrayIndex += 1) = 0x40
          currentByteIndex += 1
        }
        case 0xE0 => {
          //for (i <- 0 to endOfFirstByte + 2)
          //  resultByteArray(currentResultArrayIndex += 1) = 0x20
          currentByteIndex += 1
        }
        case 0xF0 => {
          //for (i <- 0 to endOfFirstByte + 2)
          //  resultByteArray(currentResultArrayIndex += 1) = 0x00
          currentByteIndex += 1
        }
        case _ => {
          logger.error("Error control byte: {}", controlByte)
        }
      }

    }
    ???
  }
}
