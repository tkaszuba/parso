package com.kaszub.parso.impl

/**
  * This is a trait to store debugging info, info about errors and warnings which can be received
  * when parsing the sas7bdat file.
  */
trait ParserMessageConstants {
  /**
    * Error string if there are no available bytes in the input stream.
    */
  val EMPTY_INPUT_STREAM = "There are no available bytes in the input stream."
  /**
    * Error string if the sas7bdat file is invalid.
    */
  val FILE_NOT_VALID = "Can not read metadata from sas7bdat file."
  /**
    * Debug info in case of an unknown subheader signature.
    */
  val UNKNOWN_SUBHEADER_SIGNATURE = "Unknown subheader signature"
  /**
    * Warn info if 'null' is provided as the file compression literal.
    */
  val NULL_COMPRESSION_LITERAL = "Null provided as the file compression literal, assuming no compression"
  /**
    * Debug info if no supported compression literal is found.
    */
  val NO_SUPPORTED_COMPRESSION_LITERAL = "No supported compression literal found, assuming no compression"
  /**
    * Error string if list of columns does not contain specified column name.
    */
  val UNKNOWN_COLUMN_NAME = "Unknown column name"
  /**
    * Debug info. Subheader count.
    */
  val SUBHEADER_COUNT = "Subheader count: {}"
  /**
    * Debug info. Block count.
    */
  val BLOCK_COUNT = "Block count: {}"
  /**
    * Debug info. Page type.
    */
  val PAGE_TYPE = "Page type: {}"
  /**
    * Debug info. Subheader process function name.
    */
  val SUBHEADER_PROCESS_FUNCTION_NAME = "Subheader process function name: {}"
  /**
    * Debug info. Column format.
    */
  val COLUMN_FORMAT = "Column format: {}"
}
