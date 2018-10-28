package com.kaszub.parso.impl

/**
* Implementation of the BIN compression algorithm which corresponds to the literal "SASYZCR2".
* Refer the documentation for further details.
* It follows the general contract provided by the interface <code>Decompressor</code>.
*
* History
*   01.08.2015 (Gabor Bakos): Replaced the implementation to an alternative
*/
object BinDecompressor extends Decompressor {

  /**
    * As described in the documentation, first 16 bits indicate which blocks are compressed.
    * Next, each block is preceded by a marker which may consist of one, two or three bytes.
    * This marker contains the information which compression is used (BIN or simple RLE) and
    * the block length.
    * <p>
    * Based on http://www.drdobbs.com/a-simple-data-compression-technique/184402606?pgno=2
    *
    * @param pageoffset   the offset of bytes array in <code>page</code> that contains compressed data.
    * @param srcLength    the length of bytes array that contains compressed data.
    * @param resultLength the length of bytes array that contains decompressed data.
    * @param page         an array of bytes with compressed data.
    * @return decompressed row
    */
  @Override
  def decompressRow(offset: Int, length: Int, resultLength: Int, page: Seq[Byte]): Seq[Byte] = ???

}
