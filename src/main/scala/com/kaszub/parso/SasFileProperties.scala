package com.kaszub.parso

import java.time.ZonedDateTime

import com.kaszub.parso.MissingInfoType.MissingInfoType
import com.kaszub.parso.impl.SasFileParser
import com.kaszub.parso.impl.SasFileParser.SubheaderPointer

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
case class SasFileProperties(isU64: Boolean = false, compressionMethod: Option[String] = None, endianness: Int = 0,
                             encoding: String = null, sessionEncoding: String = null, name: String = null,
                             fileType: String = null, dateCreated: ZonedDateTime = null, dateModified: ZonedDateTime = null,
                             sasRelease: String = null, serverType: String = null, osName: String = null,
                             osType: String = null, headerLength: Int = 0, pageLength: Int = 0,
                             pageCount: Long = 0L, rowLength: Long = 0L, rowCount: Long = 0L,
                             mixPageRowCount: Long = 0L, columnsCount: Long = 0L, columnsNamesBytes: Seq[Seq[Byte]] = Seq(Seq()),
                             columnNames: Seq[Either[String, ColumnMissingInfo]] = Seq(),
                             columnAttributes: Seq[ColumnAttributes] = Seq(), columnFormats: Seq[ColumnFormat] = Seq(),
                             columnLabels: Seq[ColumnLabel] = Seq(), columns: Seq[Column] = Seq(),
                             dataSubheaderPointers: Seq[SubheaderPointer] = Seq(),row: Seq[Any] = Seq()) {

  /**
    * Perform a copy of the properties
    *
    * @return result new Sas file properties.
    */
  def copy(isU64: Boolean = this.isU64, compressionMethod: Option[String] = this.compressionMethod,
                   endianness: Int = this.endianness, encoding: String = this.encoding,
                   sessionEncoding: String = this.sessionEncoding, name: String = this.name,
                   fileType: String = this.fileType, dateCreated: ZonedDateTime = this.dateCreated,
                   dateModified: ZonedDateTime = this.dateModified, sasRelease: String = this.sasRelease,
                   serverType: String = this.serverType, osName: String = this.osName,
                   osType: String = this.osType, headerLength: Int = this.headerLength,
                   pageLength: Int = this.pageLength, pageCount: Long = this.pageCount,
                   rowLength: Long = this.rowLength, rowCount: Long = this.rowCount,
                   mixPageRowCount: Long = this.mixPageRowCount, columnsCount: Long = this.columnsCount,
                   columnNamesBytes: Seq[Seq[Byte]] = this.columnsNamesBytes,
                   columnNames: Seq[Either[String, ColumnMissingInfo]] = this.columnNames,
                   columnAttributes: Seq[ColumnAttributes] = this.columnAttributes,
                   columnFormats: Seq[ColumnFormat] = this.columnFormats, columnLabels: Seq[ColumnLabel] = this.columnLabels,
                   columns: Seq[Column] = this.columns,
                   dataSubheaderPointers: Seq[SubheaderPointer] = this.dataSubheaderPointers, row: Seq[Any] = this.row
          ) =
    SasFileProperties(
      isU64, compressionMethod, endianness, encoding, sessionEncoding, name, fileType, dateCreated, dateModified,
      sasRelease, serverType, osName, osType, headerLength, pageLength, pageCount, rowLength, rowCount,
      mixPageRowCount, columnsCount, columnNamesBytes, columnNames, columnAttributes,
      columnFormats, columnLabels, columns, dataSubheaderPointers, row
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
  def setCompressionMethod(value: Option[String]): SasFileProperties = copy(compressionMethod = value)

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
  def setColumnsCount(value: Long): SasFileProperties = copy(columnsCount = value, columns = Seq())

  /**
    * Set the data subheader pointers and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setDataSubheaderPointers(value: Seq[SubheaderPointer]): SasFileProperties = copy(dataSubheaderPointers = value)

  /**
    * Set the column names bytes and return new properties. Setting this property will invalidate the column cache if it exists
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumnsNamesBytes(value: Seq[Seq[Byte]]): SasFileProperties = copy(columnNamesBytes = value, columns = Seq())

  /**
    * Set the column names and return new properties. Setting this property will invalidate the column cache if it exists
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumnsNames(value: Seq[Either[String, ColumnMissingInfo]]): SasFileProperties =
    copy(columnNames = value, columns = Seq() )

  /**
    * Set the column attributes and return new properties. Setting this property will invalidate the column cache if it exists
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumnsAttributes(value: Seq[ColumnAttributes]): SasFileProperties = copy(columnAttributes = value, columns = Seq())

  /**
    * Set the last read column formats and return new properties. Setting this property will invalidate the column cache if it exists
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumnFormats(value: Seq[ColumnFormat]): SasFileProperties = copy(columnFormats = value, columns = Seq())

  /**
    * Set the last read column labels and return new properties. Setting this property will invalidate the column cache if it exists
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumnLabels(value: Seq[ColumnLabel]): SasFileProperties = copy(columnLabels = value, columns = Seq())

  /**
    * Set the last read column and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setRow(value: Seq[Any]): SasFileProperties = copy(row = value)

  /**
    * Set the columns and return new properties
    *
    * @param val value to be set.
    * @return result new Sas file properties.
    */
  def setColumns(value: Seq[Column]): SasFileProperties = copy(columns = value)

  def isCompressed: Boolean = compressionMethod != null  && compressionMethod.isDefined

  /**
    * Get the list of columns
    *
    * @param useCache whether to return the cached columns if they exist, default is true
    * @param handleMissingColumns whether to handle missing columns, default is false
    * @return result the list of columns
    */
  def getColumns(useCache:Boolean = true, handleMissingColumns: Boolean = false): Seq[Column] = {
    if (useCache && !columns.isEmpty) return columns

    def getMissingInfo(info: ColumnMissingInfo): String =
      SasFileParser.bytesToString(columnsNamesBytes(info.textSubheaderIndex),
        info.offset, info.length, encoding).intern

    (0 until columnsCount.toInt).map(i =>
      Column(
        Some(i.toInt),
        columnNames(i) match {
          case Left(e) => Some(e)
          case Right(e) => if (handleMissingColumns) Some(getMissingInfo(e)) else None
        },
        ColumnLabel(
          columnLabels(i).alias match {
            case Left(e) => Left(e)
            case Right(e) => if (handleMissingColumns) Left(getMissingInfo(e)) else Right(e)
        }),
        ColumnFormat (
          columnFormats(i).name match {
            case Left(e) => Left(e)
            case Right(e) => if (handleMissingColumns) Left(getMissingInfo(e)) else Right(e)
          }, columnFormats(i).width, columnFormats(i).precision),
        Some(columnAttributes(i)._type),
        Some(columnAttributes(i).length)
      )
    )
  }

}
