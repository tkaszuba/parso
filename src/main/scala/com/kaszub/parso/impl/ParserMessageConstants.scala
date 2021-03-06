package com.kaszub.parso.impl

/**
  * This is a trait to store debugging info, info about errors and warnings which can be received
  * when parsing the sas7bdat file.
  */
trait ParserMessageConstants {
  /**
    * Error string if there are no available bytes in the input stream.
    */
  val EmptyInputStream = "There are no available bytes in the input stream."
  /**
    * Error string if the sas7bdat file is invalid.
    */
  val FileNotValid = "Can not read metadata from sas7bdat file."
  /**
    * Debug info in case of an unknown subheader signature.
    */
  val UnknownSubheaderSignature = "Unknown subheader signature"
  /**
    * Warn info if 'null' is provided as the file compression literal.
    */
  val NullCompressionLiteral = "Null provided as the file compression literal, assuming no compression"
  /**
    * Debug info if no supported compression literal is found.
    */
  val NoSupportedCompressionLiteral = "No supported compression literal found, assuming no compression"
  /**
    * Error string if list of columns does not contain specified column name.
    */
  val UnknownColumnName = "Unknown column name"
  /**
    * Debug info. Subheader count.
    */
  val SubheaderCount = "Subheader count: %s"
  /**
    * Debug info. Block count.
    */
  val BlockCount = "Block count: %s"
  /**
    * Debug info. Page type.
    */
  val PageType = "Page type: %s"
  /**
    * Debug info. Subheader process function name.
    */
  val SubheaderProcessFunctionName = "Subheader process function name: %s"
  /**
    * Debug info. Column format.
    */
  val ColumnFormatMsg = "Column format: %s"
}
