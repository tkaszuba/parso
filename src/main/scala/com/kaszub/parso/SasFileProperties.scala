package com.kaszub.parso

import java.time.{ZonedDateTime}

/**
  * A class to store all the sas7bdat file metadata.
  *
  * @param u64               The flag of the 64-bit version of SAS in which the sas7bdat file was created;
  *                          false means the 32-bit version, true means the 64-bit version.
  * @param compressionMethod Compression method used for the sas7bdat file.
  * @param endianness        The bytes sequence; 1 sets the little-endian sequence (Intel), 0 sets big-endian.
  * @param encoding          The name of the sas7bdat character encoding.
  * @param sessionEncoding   The name of the sas7bdat character encoding.
  * @param name              The name of the sas7bdat file table.
  * @param fileType          The type of the sas7bdat file.
  * @param dateCreated       The date of the sas7bdat file creation.
  * @param dateModified      The date of the last modification of the sas7bdat file.
  * @param sasRelease        The version of SAS in which the sas7bdat was created.
  * @param serverType        The version of the server on which the sas7bdat was created.
  * @param osName            The name of the OS in which the sas7bdat file was created.
  * @param osType            The version of the OS in which the sas7bdat file was created.
  * @param headerLength      The number of bytes the sas7bdat file metadata takes.
  * @param pageLength        The number of bytes each page of the sas7bdat file takes.
  * @param pageCount         The number of pages in the sas7bdat file.
  * @param rowLength         The number of bytes every row of the table takes.
  * @param rowCount          The number of rows in the table.
  * @param mixPageRowCount   The number of rows on the page containing both data and metadata.
  * @param columnsCount      The number of columns in the table.
  *
  */
case class SasFileProperties(isU64: Boolean = false, compressionMethod: String = null, endianness: Int = 0,
                             encoding: String = null, sessionEncoding: String = null, name: String = null,
                             fileType: String = null, dateCreated: ZonedDateTime = null, dateModified: ZonedDateTime = null,
                             sasRelease: String = null, serverType: String = null, osName: String = null,
                             osType: String = null, headerLength: Int = 0, pageLength: Int = 0,
                             pageCount: Long = 0L, rowLength: Long = 0L, rowCount: Long = 0L,
                             mixPageRowCount: Long = 0L, columnsCount: Long = 0L) {

  /**
    * Perform a copy of the properties
    *
    * @return result new Sas file properties.
    */
  private def copy(isU64: Boolean = this.isU64, compressionMethod: String = this.compressionMethod,
                   endianness: Int = this.endianness, encoding: String = this.encoding,
                   sessionEncoding: String = this.sessionEncoding, name: String = this.name,
                   fileType: String = this.fileType, dateCreated: ZonedDateTime = this.dateCreated,
                   dateModified: ZonedDateTime = this.dateModified, sasRelease: String = this.sasRelease,
                   serverType: String = this.serverType, osName: String = this.osName,
                   osType: String = this.osType, headerLength: Int = this.headerLength,
                   pageLength: Int = this.pageLength, pageCount: Long = this.pageCount,
                   rowLength: Long = this.rowLength, rowCount: Long = this.rowCount,
                   mixPageRowCount: Long = this.mixPageRowCount, columnsCount: Long = this.columnsCount) =
    SasFileProperties(
      isU64, compressionMethod, endianness, encoding, sessionEncoding, name, fileType, dateCreated, dateModified,
      sasRelease, serverType, osName, osType, headerLength, pageLength, pageCount, rowLength, rowCount,
      mixPageRowCount, columnsCount
    )

  /**
    * Set isU64 and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setIsU64(value: Boolean): SasFileProperties = copy(isU64 = value)

  /**
    * Set the compression method and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setCompressionMethod(value: String): SasFileProperties = copy(compressionMethod = value)

  /**
    * Set the compression method and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  //TODO: This should be a bit not an int
  def setEndianness(value: Int): SasFileProperties = copy(endianness = value)

  /**
    * Set the encoding and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setEncoding(value: String): SasFileProperties = copy(encoding = value)

  /**
    * Set the row length and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setRowLength(value: Long): SasFileProperties = copy(rowLength = value)

  /**
    * Set the row count and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setRowCount(value: Long): SasFileProperties = copy(rowCount = value)

  /**
    * Set the mix page row count and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setMixPageRowCount(value: Long): SasFileProperties = copy(mixPageRowCount = value)

  /**
    * Set the column count and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumnsCount(value: Long): SasFileProperties = copy(columnsCount = value)


  def isCompressed: Boolean = compressionMethod != null

}
