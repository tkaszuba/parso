package com.kaszub.parso.impl

/**
  * Interface which should be implemented in all data decompressors.
  */
trait Decompressor {
  /**
    * The function to decompress data. Compressed data are an array of bytes with control bytes and data bytes.
    * The project documentation contains descriptions of the decompression algorithm.
    *
    * @param offset       the offset of bytes array in <code>page</code> that contains compressed data.
    * @param srcLength    the length of bytes array that contains compressed data.
    * @param resultLength the length of bytes array that contains decompressed data.
    * @param page         an array of bytes with compressed data.
    * @return an array of bytes with decompressed data.
    */
  def decompressRow(offset: Int, srcLength: Int, resultLength: Int, page: Seq[Byte]): Seq[Byte]
}
