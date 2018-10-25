package com.kaszub.parso.impl

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.kaszub.parso.DataWriterUtil.convertDoubleElementToString
import com.kaszub.parso.impl.SasFileParser.SubheaderIndexes
import com.kaszub.parso.impl.SasFileParser.SubheaderIndexes.SubheaderIndexes
import com.kaszub.parso._
import com.typesafe.scalalogging.Logger

import scala.io.Source
import scala.util.control.Breaks._

case class SasFileParser private (val sasFileStream : BufferedInputStream = null,
                         encoding : String = null,
                         byteOutput: Boolean = false
                        ) extends SasFileConstants with ParserMessageConstants {

  /**
    * Object for writing logs.
    */
  private val logger = Logger[this.type]

  /**
    * The list of current page data subheaders.
    */
  //private val currentPageDataSubheaderPointers = Seq[SubheaderPointer]()

  /**
    * The variable to store all the properties from the sas7bdat file.
    */
  //TODO: Refactor to be non variable
  private var sasFileProperties = SasFileProperties()

  /**
    * The list of text blocks with information about file compression and table columns (name, label, format).
    * Every element corresponds to a {@link SasFileParser.ColumnTextSubheader}. The first text block includes
    * the information about compression.
    */
  private val columnsNamesBytes = Seq[Seq[Byte]]()

  /**
    * The list of column names.
    */
  private val columnsNamesList = Seq[String]()

  /**
    * The list of column types. There can be {@link Number} and {@link String} types.
    */
  private val columnsTypesList = Seq[Class[_]]()

  /**
    * The list of offsets of data in every column inside a row. Used to locate the left border of a cell.
    */
  private val columnsDataOffset = Seq[Long]()

  /**
    * The list of data lengths of every column inside a row. Used to locate the right border of a cell.
    */
  private val columnsDataLength = Seq[Int]()

  /**
    * The list of table columns to store their name, label, and format.
    */
  private val columns = Seq[Column]()

  /**
    * A cache to store the current page of the sas7bdat file. Used to avoid posing buffering requirements
    * to {@link SasFileParser#sasFileStream}.
    */
  //todo: make non variable
  private var cachedPage = Array[Byte]()

  /**
    * The type of the current page when reading the file. If it is other than
    * {@link SasFileConstants#PAGE_META_TYPE_1}, {@link SasFileConstants#PAGE_META_TYPE_1},
    * {@link SasFileConstants#PAGE_MIX_TYPE} and {@link SasFileConstants#PAGE_DATA_TYPE} page is skipped.
    */
  private val currentPageType = 0

  /**
    * Number current page blocks.
    */
  private val currentPageBlockCount = 0

  /**
    * Number current page subheaders.
    */
  private val currentPageSubheadersCount = 0

  /**
    * The index of the current byte when reading the file.
    */
  private val currentFilePosition = 0

  /**
    * The index of the current column when reading the file.
    */
  private val currentColumnNumber = 0

  /**
    * The index of the current row when reading the file.
    */
  private val currentRowInFileIndex = 0

  /**
    * The index of the current row when reading the page.
    */
  private val currentRowOnPageIndex = 0

  /**
    * Last read row from sas7bdat file.
    */
  private val currentRow = Seq[Any]()

  /**
    * True if stream is at the end of file.
    */
  //todo: Make non variable
  private var eof = false

  /**
    * The list of missing column information.
    */
  private val columnMissingInfoList = Seq[ColumnMissingInfo]()

  private def this(builder: SasFileParser#Builder) = {
    this(new BufferedInputStream(builder.inputStream), builder.encoding, builder.byteOutput)


    /*
    import java.io.IOException
try  { getMetadataFromSasFile
} catch {
case e: IOException =>
LOGGER.error(e.getMessage, e)
}
     */


  }

  /**
    * The method that reads and parses metadata from the sas7bdat and puts the results in
    * {@link SasFileParser#sasFileProperties}.
    *
    * @throws IOException - appears if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  //todo: refactor to use tail recursion
  private def getMetadataFromSasFile(): Unit = {
    processSasFileHeader(sasFileProperties)
    cachedPage = new Array[Byte](sasFileProperties.pageLength)

    breakable {
      var endOfMetadata = false
      while (!endOfMetadata) {
        try
          sasFileStream.read(cachedPage, 0, sasFileProperties.pageLength)
        catch {
          case ex: EOFException =>
            eof = true
            break
        }
        //endOfMetadata = processSasFilePageMeta
      }
    }
  }

  /**
    * The method to read and parse metadata from the sas7bdat file`s header in {@link SasFileParser#sasFileProperties}.
    * After reading is complete, {@link SasFileParser#currentFilePosition} is set to the end of the header whose length
    * is stored at the {@link SasFileConstants#HEADER_SIZE_OFFSET} offset.
    *
    * @throws IOException if reading from the {@link SasFileParser#sasFileStream} stream is impossible.
    **/
  @throws[IOException]
  private def processSasFileHeader(properties: SasFileProperties): Unit = {
    ???
  }

  /**
    * The method to read page metadata and store it in {@link SasFileParser#currentPageType},
    * {@link SasFileParser#currentPageBlockCount} and {@link SasFileParser#currentPageSubheadersCount}.
    *
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} string is impossible.
    */
  @throws[IOException]
  private def readPageHeader(): Unit = {
    val bitOffset = if (sasFileProperties.isU64) PageBitOffsetX64 else PageBitOffsetX86
    val offset = Seq(bitOffset + PAGE_TYPE_OFFSET, bitOffset + BLOCK_COUNT_OFFSET, bitOffset + SUBHEADER_COUNT_OFFSET)
    val length = Seq(PAGE_TYPE_LENGTH, BLOCK_COUNT_LENGTH, SUBHEADER_COUNT_LENGTH)
    val vars = getBytesFromFile(offset, length)

    ???
    /*currentPageType = bytesToShort(vars(0))
    logger.debug(PAGE_TYPE, currentPageType)
    currentPageBlockCount = bytesToShort(vars(1))
    logger.debug(BLOCK_COUNT, currentPageBlockCount)
    currentPageSubheadersCount = bytesToShort(vars(2))
    logger.debug(SUBHEADER_COUNT, currentPageSubheadersCount)*/
  }

  /**
    * The function to convert the array of bytes that stores the data of a row into an array of objects.
    * Each object corresponds to a table cell.
    *
    * @param rowOffset   - the offset of the row in cachedPage.
    * @param rowLength   - the length of the row.
    * @param columnNames - list of column names which should be processed.
    * @return the array of objects storing the data of the row.
    */
  private def processByteArrayWithData(rowOffset: Long, rowLength: Long, columnNames: Seq[String]) = ???

  /**
    * The function to process element of row.
    *
    * @param source             an array of bytes containing required data.
    * @param offset             the offset in source of required data.
    * @param currentColumnIndex index of the current element.
    * @return object storing the data of the element.
    */
  private def processElement(source: Seq[Byte], offset: Int, currentColumnIndex: Int) = ???

  @throws[IOException]
  private def getBytesFromFile(offset: Seq[Long], length: Seq[Int]): Seq[Seq[Byte]] = {
    if (cachedPage == null) {
      val res = SasFileParser.getBytesFromFile(sasFileStream, currentFilePosition, offset, length)
      eof = res.eof
      res.readBytes
    }
    else
      SasFileParser.getBytesFromFile(cachedPage, offset, length)
  }

  /**
  * The function to get sasFileParser.
  *
  * @return the object of the {@link SasFileProperties} class that stores file metadata.
  */
  def getSasFileProperties:SasFileProperties = sasFileProperties

  /**
  * SasFileParser builder class made using builder pattern.
  */
  case class Builder (inputStream: InputStream = null, encoding: String = "US-ASCII", byteOutput: Boolean = false){

    /**
      * The function to specify builders sasFileStream variable.
      *
      * @param val value to be set.
      * @return result builder.
      */
    def sasFileStream(value : InputStream): Builder = Builder(value)

    /**
      * The function to specify builders encoding variable.
      *
      * @param val value to be set.
      * @return result builder.
      */
    def encoding(value : String): Builder = Builder(inputStream, value)

    /**
      * The function to specify builders byteOutput variable.
      *
      * @param val value to be set.
      * @return result builder.
      */
    def byteOutput(value : Boolean): Builder = Builder(inputStream, encoding, value)

    /**
      * The function to create variable of SasFileParser class using current builder.
      *
      * @return newly built SasFileParser
      */
    def build : SasFileParser = new SasFileParser(this)
  }

}

object SasFileParser extends ParserMessageConstants with SasFileConstants {

  /**
    * Object for writing logs.
    */
  private val logger = Logger[SasFileParser.type]

  /**
    * The mapping of the supported string literals to the compression method they mean.
    */
  private val LiteralsToDecompressor = Map[String, Decompressor](
    COMPRESS_CHAR_IDENTIFYING_STRING -> CharDecompressor,
    //COMPRESS_BIN_IDENTIFYING_STRING -> BinDecompressor
  )

  /**
    * The mapping between elements from {@link SubheaderIndexes} and classes corresponding
    * to each subheader. This is necessary because defining the subheader type being processed is dynamic.
    * Every class has an overridden function that processes the related subheader type.
    */
  val subheaderIndexToClass = Map[SubheaderIndexes.Value, ProcessingSubheader](
    SubheaderIndexes.RowSizeSubheaderIndex -> RowSizeSubheader(),
    SubheaderIndexes.ColumnSizeSubheaderIndex -> ColumnSizeSubheader(),
    SubheaderIndexes.SubheaderCountsSubheaderIndex -> SubheaderCountsSubheader(),
    SubheaderIndexes.ColumnTextSubheaderIndex -> ColumnTextSubheader(),
    SubheaderIndexes.ColumnNameSubheaderIndex -> ColumnNameSubheader(),
    SubheaderIndexes.ColumnAttributesSubheaderIndex -> ColumnAttributesSubheader(),
    //SubheaderIndexes.FormatAndLabelSubheaderIndex -> FormatAndLabelSubheader(),
    //SubheaderIndexes.ColumnListSubheaderIndex -> ColumnListSubheader(),
    //SubheaderIndexes.DataSubheaderIndex -> DataSubheader()
  )

  /**
    * The mapping of subheader signatures to the corresponding elements in {@link SubheaderIndexes}.
    * Depending on the value at the {@link SasFileConstants#ALIGN_2_OFFSET} offset, signatures take 4 bytes
    * for 32-bit version sas7bdat files and 8 bytes for the 64-bit version files.
    */
  private val SubheaderSignatureToIndex = Map[Long, SubheaderIndexes.Value](
    0xF7F7F7F7.toLong -> SubheaderIndexes.RowSizeSubheaderIndex,
    0xF6F6F6F6.toLong -> SubheaderIndexes.ColumnSizeSubheaderIndex,
    0xFFFFFC00.toLong -> SubheaderIndexes.SubheaderCountsSubheaderIndex,
    0xFFFFFFFD.toLong -> SubheaderIndexes.ColumnTextSubheaderIndex,
    0xFFFFFFFF.toLong -> SubheaderIndexes.ColumnNameSubheaderIndex,
    0xFFFFFFFC.toLong -> SubheaderIndexes.ColumnAttributesSubheaderIndex,
    0xFFFFFBFE.toLong -> SubheaderIndexes.FormatAndLabelSubheaderIndex,
    0xFFFFFFFE.toLong -> SubheaderIndexes.ColumnListSubheaderIndex,
    0x00000000F7F7F7F7L -> SubheaderIndexes.RowSizeSubheaderIndex,
    0x00000000F6F6F6F6L -> SubheaderIndexes.ColumnSizeSubheaderIndex,
    0xF7F7F7F700000000L -> SubheaderIndexes.RowSizeSubheaderIndex,
    0xF6F6F6F600000000L -> SubheaderIndexes.ColumnSizeSubheaderIndex,
    0xF7F7F7F7FFFFFBFEL -> SubheaderIndexes.RowSizeSubheaderIndex,
    0xF6F6F6F6FFFFFBFEL -> SubheaderIndexes.ColumnSizeSubheaderIndex,
    0x00FCFFFFFFFFFFFFL -> SubheaderIndexes.SubheaderCountsSubheaderIndex,
    0xFDFFFFFFFFFFFFFFL -> SubheaderIndexes.ColumnTextSubheaderIndex,
    0xFFFFFFFFFFFFFFFFL -> SubheaderIndexes.ColumnNameSubheaderIndex,
    0xFCFFFFFFFFFFFFFFL -> SubheaderIndexes.ColumnAttributesSubheaderIndex,
    0xFEFBFFFFFFFFFFFFL -> SubheaderIndexes.FormatAndLabelSubheaderIndex,
    0xFEFFFFFFFFFFFFFFL -> SubheaderIndexes.ColumnListSubheaderIndex
  )

  /**
    * The function to convert a bytes array into a number (int or long depending on the value located at
    * the {@link SasFileConstants#ALIGN_2_OFFSET} offset).
    *
    * @param byteBuffer the long value represented by a bytes array.
    * @return a long value. If the number was stored as int, then after conversion it is converted to long
    *         for convenience.
    */
  private def correctLongProcess(byteBuffer: ByteBuffer, isU64: Boolean): Long =
    if (isU64) byteBuffer.getLong else byteBuffer.getInt

  /**
    * The function to convert an array of bytes with any order of bytes into {@link ByteBuffer}.
    * {@link ByteBuffer} has the order of bytes defined in the file located at the
    * {@link SasFileConstants#ALIGN_2_OFFSET} offset.
    * Later the parser converts result {@link ByteBuffer} into a number.
    *
    * @param data the input array of bytes with the little-endian or big-endian order.
    * @return {@link ByteBuffer} with the order of bytes defined in the file located at
    *                 the {@link SasFileConstants#ALIGN_2_OFFSET} offset.
    */
  def byteArrayToByteBuffer(data: Seq[Byte], endianness: Int): ByteBuffer = {
    val byteBuffer = ByteBuffer.wrap(data.toArray)
    if (endianness == 0)
      byteBuffer
    else
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
  }

  /**
    * The function to convert an array of bytes into a number. The result can be double or long values.
    * The numbers are stored in the IEEE 754 format. A number is considered long if the difference between the whole
    * number and its integer part is less than {@link SasFileConstants#EPSILON}.
    *
    * @param mass the number represented by an array of bytes.
    * @return number of a long or double type.
    */
  private def convertByteArrayToNumber(mass : Seq[Byte], endianness: Int): Any = {
    val resultDouble: Double = bytesToDouble(mass, endianness)

    if (resultDouble.isNaN || (resultDouble < NAN_EPSILON && resultDouble > 0)) null

    val resultLong: Long = Math.round(resultDouble)

    if (Math.abs(resultDouble - resultLong) >= EPSILON)
      resultDouble
    else
      resultLong
  }

  /**
    * The function to convert an array of bytes into a numeral of the {@link Short} type.
    * For convenience, the resulting number is converted into the int type.
    *
    * @param bytes a long number represented by an array of bytes.
    * @return a number of the int type that is the conversion result.
    */
  def bytesToShort(bytes: Seq[Byte], endianness: Int): Int =
    byteArrayToByteBuffer(bytes, endianness).getShort

  /**
    * The function to convert an array of bytes into an int number.
    *
    * @param bytes a long number represented by an array of bytes.
    * @return a number of the int type that is the conversion result.
    */
  def bytesToInt(bytes: Seq[Byte], endianness: Int): Int =
    byteArrayToByteBuffer(bytes, endianness).getInt

  /**
    * The function to convert an array of bytes into a long number.
    *
    * @param bytes a long number represented by an array of bytes.
    * @return a number of the long type that is the conversion result.
    */
  def bytesToLong(bytes: Seq[Byte], isU64: Boolean, endianness: Int): Long =
    correctLongProcess(byteArrayToByteBuffer(bytes, endianness), isU64)

  /**
    * The function to convert an array of bytes into a string.
    *
    * @param bytes a string represented by an array of bytes.
    * @return the conversion result string.
    */
  def bytesToString(bytes: Seq[Byte], encoding: String): String =
    Source.fromBytes(bytes.toArray, encoding).mkString

  /**
    * The function to convert a sub-range of an array of bytes into a string.
    *
    * @param bytes  a string represented by an array of bytes.
    * @param offset the initial offset
    * @param length the length
    * @return the conversion result string.
    * @throws UnsupportedEncodingException    when unknown encoding.
    * @throws StringIndexOutOfBoundsException when invalid offset and/or length.
    */
  @throws[UnsupportedEncodingException]
  @throws[StringIndexOutOfBoundsException]
  def bytesToString(bytes: Seq[Byte], offset: Int, length: Int, encoding: String): String =
    new java.lang.String(bytes.toArray, offset, length, encoding)

  /**
    * The function to convert an array of bytes that stores the number of seconds elapsed from 01/01/1960 into
    * a variable of the {@link Date} type. The {@link SasFileConstants#DATE_TIME_FORMAT_STRINGS} variable stores
    * the formats of the columns that store such data.
    *
    * @param bytes an array of bytes that stores the type.
    * @return a variable of the { @link Date} type.
    */
  def bytesToDateTime(bytes: Seq[Byte], endianness: Int): ZonedDateTime = {
    val doubleSeconds: Double = bytesToDouble(bytes, endianness)
    if (doubleSeconds.isNaN)
      null
    else
      Instant.ofEpochSecond(
        (doubleSeconds - StartDatesSecondsDifference).toLong).atOffset(ZoneOffset.UTC).toZonedDateTime
  }

  /**
    * The function to convert an array of bytes that stores the number of days elapsed from 01/01/1960 into a variable
    * of the {@link Date} type. {@link SasFileConstants#DATE_FORMAT_STRINGS} stores the formats of columns that contain
    * such data.
    *
    * @param bytes the array of bytes that stores the number of days from 01/01/1960.
    * @return a variable of the { @link Date} type.
    */
  def bytesToDate(bytes: Seq[Byte], endianness: Int): ZonedDateTime = {
    val doubleDays: Double = bytesToDouble(bytes, endianness)
    if (doubleDays.isNaN)
      null
    else
      Instant.ofEpochMilli(
        ((doubleDays - StartDatesDaysDifference) *
          SecondsInMinute *
          MinutesInHour *
          HoursInDay).toLong).atOffset(ZoneOffset.UTC).toZonedDateTime
  }

  /**
    * The function to convert an array of bytes into a double number.
    *
    * @param bytes a double number represented by an array of bytes.
    * @return a number of the double type that is the conversion result.
    */
  def bytesToDouble(bytes: Seq[Byte], endianness: Int): Double = {

    val original = byteArrayToByteBuffer(bytes, endianness)

    if (bytes.length < BytesInDouble) {
      val byteBuffer = ByteBuffer.allocate(BytesInDouble)

      if (endianness == LittleEndianChecker)
        byteBuffer.position(BytesInDouble - bytes.length)

      byteBuffer.put(original)
      byteBuffer.order(original.order)
      byteBuffer.position(0)
      byteBuffer.getDouble
    }
    else
      original.getDouble
  }

  /**
    * The class to store subheaders pointers that contain information about the offset, length, type
    * and compression of subheaders (see {@link SasFileConstants#TruncatedSubheaderId},
    * {@link SasFileConstants#CompressedSubheaderId}, {@link SasFileConstants#CompressedSubheaderType}
    * for details).
    *
    * @param offset The offset from the beginning of a page at which a subheader is stored.
    * @param length The subheader length
    * @param compression The type of subheader compression. If the type is {@link SasFileConstants#TruncatedSubheaderId}
    *    the subheader does not contain information relevant to the current issues. If the type is
    *    {@link SasFileConstants#CompressedSubheaderId} the subheader can be compressed (depends on {@link SubheaderPointer#_type})
    * @param _type The subheader type. If the type is {@link SasFileConstants#CompressedSubheaderType} the subheader
    *      is compressed. Otherwise, there is no compression.
    */
  case class SubheaderPointer(offset: Long, length: Long, compression: Byte, _type: Byte)

  /**
    * Enumeration of missing information types.
    */
  object SubheaderIndexes extends Enumeration {
    type SubheaderIndexes = Value
    /**
      * Index which define row size subheader, which contains rows size in bytes and the number of rows.
      */
    val RowSizeSubheaderIndex = Value

    /**
      * Index which define column size subheader, which contains columns count.
      */
    val ColumnSizeSubheaderIndex = Value

    /**
      * Index which define subheader counts subheader, which contains currently not used data.
      */
    val SubheaderCountsSubheaderIndex = Value

    /**
      * Index which define column text subheader, which contains type of file compression
      * and info about columns (name, label, format).
      */
    val ColumnTextSubheaderIndex = Value

    /**
      * Index which define column name subheader, which contains column names.
      */
    val ColumnNameSubheaderIndex = Value

    /**
      * Index which define column attributes subheader, which contains column attributes, such as type.
      */
    val ColumnAttributesSubheaderIndex = Value

    /**
      * Index which define format and label subheader, which contains info about format of objects in column
      * and tooltip text for columns.
      */
    val FormatAndLabelSubheaderIndex = Value

    /**
      * Index which define column list subheader, which contains currently not used data.
      */
    val ColumnListSubheaderIndex = Value

    /**
      * Index which define data subheader, which contains sas7bdat file rows data.
      */
    val DataSubheaderIndex = Value
  }

  /**
    * The trait that is implemented by all classes that process subheaders.
    */
  trait ProcessingSubheader {
    /**
      * Method which should be overwritten in implementing this interface classes.
      *
      * @param subheaderOffset offset in bytes from the beginning of subheader.
      * @param subheaderLength length of subheader in bytes.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    @throws[IOException]
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties
  }

  /**
    * The trait that is implemented by classes that process data subheader.
    */
  trait ProcessingDataSubheader extends ProcessingSubheader{
    /**
      * Method which should be overwritten in implementing this interface classes.
      *
      * @param subheaderOffset offset in bytes from the beginning of subheader.
      * @param subheaderLength length of subheader in bytes.
      * @param columnNames     list of column names which should be processed.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    @throws[IOException]
    def processSubheader(subheaderOffset: Long, subheaderLength: Long, columnNames: Seq[String]): Unit
  }

  /**
    * The class to process subheaders of the RowSizeSubheader type that store information about the table rows length
    * (in bytes), the number of rows in the table and the number of rows on the last page of the
    * {@link SasFileConstants#PAGE_MIX_TYPE} type. The results are stored in {@link SasFileProperties#rowLength},
    * {@link SasFileProperties#rowCount}, and {@link SasFileProperties#mixPageRowCount}, respectively.
    */
  case class RowSizeSubheader() extends ProcessingSubheader {
    /**
      * The function to read the following metadata about rows of the sas7bdat file:
      * {@link SasFileProperties#rowLength}, {@link SasFileProperties#rowCount},
      * and {@link SasFileProperties#mixPageRowCount}.
      *
      * @param offset the offset at which the subheader is located.
      * @param length the subheader length.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    @throws[IOException]
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val offsetSubheader = Seq(
        offset + ROW_LENGTH_OFFSET_MULTIPLIER * intOrLongLength,
        offset + ROW_COUNT_OFFSET_MULTIPLIER * intOrLongLength,
        offset + ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER * intOrLongLength)
      val offsetLength = Seq(intOrLongLength, intOrLongLength, intOrLongLength)

      val vars = getBytesFromFile(inputStream, 0, offsetSubheader, offsetLength).readBytes

      val isU64 = properties.isU64
      val endianness = properties.endianness

      val propertiesRowLength =
        if (properties.rowLength == 0)
          properties.setRowLength(SasFileParser.bytesToLong(vars(0), isU64, endianness))
        else
          properties

      val propertiesRowCount =
        if (properties.rowCount == 0)
          propertiesRowLength.setRowCount(SasFileParser.bytesToLong(vars(1), isU64, endianness))
        else
          propertiesRowLength

      val propertiesMixPageRowCount =
        if (properties.mixPageRowCount == 0)
          propertiesRowCount.setMixPageRowCount(SasFileParser.bytesToLong(vars(2), isU64, endianness))
        else
          propertiesRowCount

      propertiesMixPageRowCount
    }
  }

  /**
    * The class to process subheaders of the ColumnSizeSubheader type that store information about
    * the number of table columns. The {@link SasFileProperties#columnsCount} variable stores the results.
    */
  case class ColumnSizeSubheader() extends ProcessingSubheader {
    /**
      * The function to read the following metadata about columns of the sas7bdat file:
      * {@link SasFileProperties#columnsCount}.
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    @throws[IOException]
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val offsetSubheader = Seq(offset + intOrLongLength)
      val lengthSubheader = Seq(intOrLongLength)

      val vars = getBytesFromFile(inputStream, 0, offsetSubheader, lengthSubheader).readBytes

      properties.setColumnsCount(bytesToLong(vars(0), properties.isU64, properties.endianness))
    }
  }

  /**
    * The class to process subheaders of the SubheaderCountsSubheader type that does not contain
    * any information relevant to the current issues.
    */
  case class SubheaderCountsSubheader() extends ProcessingSubheader {
    /**
      * The function to read metadata. At the moment the function is empty as the information in
      * SubheaderCountsSubheader is not needed for the current issues.
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      * @throws IOException if reading from the {@link SasFileParser#sasFileStream} stream is impossible.
      */
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = properties
  }

  /**
    * The class to process subheaders of the ColumnTextSubheader type that store information about
    * file compression and table columns (name, label, format). The first subheader of this type includes the file
    * compression information. The results are stored in {@link SasFileParser#columnsNamesBytes} and
    * {@link SasFileProperties#compressionMethod}.
    */
  case class ColumnTextSubheader() extends ProcessingSubheader {
    /**
      * The function to read the text block with information about file compression and table columns (name, label,
      * format) from a subheader. The first text block of this type includes the file compression information.
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val offsetSubheader = Seq(offset + intOrLongLength)
      val lengthByteBuffer = Seq(TextBlockSizeLength)

      val vars = getBytesFromFile(inputStream, 0, offsetSubheader, lengthByteBuffer).readBytes
      val textBlockSize = byteArrayToByteBuffer(vars(0), properties.endianness).getShort

      val lengthFile = Seq(textBlockSize.toInt)
      val varsFile = getBytesFromFile(inputStream, 0, offsetSubheader, lengthFile).readBytes

      val columnName = varsFile(0)
      val compressionLiteral = findCompressionLiteral(bytesToString(columnName, properties.encoding))

      properties.setCompressionMethod(compressionLiteral).setColumnsNamesBytes(Seq(columnName))
    }
  }

  /**
    * The class to process subheaders of the ColumnNameSubheader type that store information about the index of
    * corresponding subheader of the ColumnTextSubheader type whose text field stores the name of the column
    * corresponding to the current subheader. They also store the offset (in symbols) of the names from the beginning
    * of the text field and the length of names (in symbols). The {@link SasFileParser#columnsNamesList} list stores
    * the resulting names.
    */
  case class ColumnNameSubheader() extends ProcessingSubheader {
    /**
      * The function to read the following data from the subheader:
      * - the index that stores the name of the column corresponding to the current subheader,
      * - the offset (in symbols) of the name inside the text block,
      * - the length (in symbols) of the name.
      *
      * @param offset the offset at which the subheader is located.
      * @param length the subheader length.
      */
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val columnNamePointersCount = (length - 2 * intOrLongLength - 12) / 8

      val columnsNames = (0 to columnNamePointersCount.toInt - 1).map(i => {
        val colOffset = Seq(
          offset + intOrLongLength + ColumnNamePointerLength * (i + 1) + ColumnNameTextSubheaderOffset,
          offset + intOrLongLength + ColumnNamePointerLength * (i + 1) + ColumnNameOffsetOffset,
          offset + intOrLongLength + ColumnNamePointerLength * (i + 1) + ColumnNameLengthOffset)
        val colLength = Seq(ColumnNameTextSubheaderLength, ColumnNameOffsetLength, ColumnNameLengthLength)

        val vars = getBytesFromFile(inputStream,0, colOffset, colLength).readBytes

        val textSubheaderIndex = bytesToShort(vars(0), properties.endianness)
        val columnNameOffset = bytesToShort(vars(1), properties.endianness)
        val columnNameLength = bytesToShort(vars(2), properties.endianness)

        if (textSubheaderIndex < properties.columnsNamesBytes.size)
          Left(
            bytesToString(properties.columnsNamesBytes(textSubheaderIndex), columnNameOffset, columnNameLength, properties.encoding).intern())
        else
          Right(
            ColumnMissingInfo(i.toInt, textSubheaderIndex, columnNameOffset, columnNameLength, MissingInfoType.NAME))
      })
      properties.setColumnsNames(columnsNames)
    }
  }

  /**
    * The class to process subheaders of the ColumnAttributesSubheader type that store information about
    * the data length (in bytes) of the current column and about the offset (in bytes) of the current column`s data
    * from the beginning of the row with data. They also store the column`s data type: {@link Number} and
    * {@link String}. The resulting names are stored in the {@link SasFileParser#columnsDataOffset},
    * {@link SasFileParser#columnsDataLength}, and{@link SasFileParser#columnsTypesList}.
    */
  case class ColumnAttributesSubheader() extends ProcessingSubheader {
    /**
      * The function to read the length, offset and type of data in cells related to the column from the subheader
      * that stores information about this column.
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      */
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val columnAttributesVectorsCount = (length - 2 * intOrLongLength - 12) / (intOrLongLength + 8)

      val attributes = (0 to columnAttributesVectorsCount.toInt - 1).map(i => {
        val attOffset = Seq(
          offset + intOrLongLength + ColumnDataOffsetOffset + i * (intOrLongLength + 8),
          offset + 2 * intOrLongLength + ColumnDataLengthOffset + i * (intOrLongLength + 8),
          offset + 2 * intOrLongLength + ColumnTypeOffset + i * (intOrLongLength + 8))
        val attLength = Seq(intOrLongLength, ColumnDataLengthLength, ColumnTypeLength)

        val vars = getBytesFromFile(inputStream,0, attOffset, attLength).readBytes

        ColumnAttributes(
          bytesToLong(vars(0), properties.isU64, properties.endianness),
          bytesToInt(vars(1), properties.endianness),
          if (vars(2)(0) == 1) classOf[Number] else classOf[String]
        )
      })
      properties.setColumnsAttributes(attributes)
    }
  }

  /**
    * The class to process subheaders of the ColumnListSubheader type that do not store any information relevant
    * to the current tasks.
    */
  case class ColumnListSubheader() extends ProcessingSubheader {
    /**
      * The method to read metadata. It is empty at the moment because the data stored in ColumnListSubheader
      * are not used.
      *
      * @param offset the offset at which the subheader is located.
      * @param length the subheader length.
      */
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = properties
  }

  /**
    * The class to process subheaders of the DataSubheader type that keep compressed or uncompressed data.
    */
  case class DataSubheader() extends ProcessingDataSubheader {
    /**
      * The method to read compressed or uncompressed data from the subheader. The results are stored as a row
      * in {@link SasFileParser#currentRow}. The {@link SasFileParser#processByteArrayWithData(long, long, List)}
      * function converts the array of bytes into a list of objects.
      *
      * @param offset the offset at which the subheader is located.
      * @param length the subheader length.
      */
    def processSubheader(inputStream: InputStream, properties: SasFileProperties, offset: Long, length: Long): SasFileProperties = {
      ???
      //currentRow = processByteArrayWithData(subheaderOffset, subheaderLength, null)
    }

    def processSubheader(subheaderOffset: Long, subheaderLength: Long, columnNames: Seq[String]): Unit = {
      ???
      ///currentRow = processByteArrayWithData(subheaderOffset, subheaderLength, columnNames)
    }
  }

  /**
    * The result of reading bytes from a file
    *
    * @param eof          whether the eof file was reached while reading the stream
    * @param absPosition the absolute position when reading the input stream
    * @param relPosition the last relative position when reading the input stream
    * @param readBytes    the matrix of read bytes
    */
  case class BytesReadResult(eof: Boolean = false, absPosition:Long = 0L, relPosition: Long = 0L, readBytes: Seq[Seq[Byte]] = Seq())

  case class MetadataReadResult(pointers: Seq[(SubheaderPointer, SasFileProperties)])

  /**
    * The result of parsing a SAS file
    *
    * @param properties the sas file properties after parsing
    * @param relPosition the last relative position when reading the input stream
    */
  //todo: move position to a separate class?
  case class ParseResult(properties: SasFileProperties = null, absPosition:Long = 0L)

  case class PageHeader(pageType: Int = 0, blockCount: Int = 0, subheaderCount: Int = 0)

  def readPage(stream: InputStream, properties: SasFileProperties) : BytesReadResult =
    getBytesFromFile(stream, 0L, Seq(0L), Seq(properties.pageLength))

  private def skipStream(stream: InputStream, skipTo: Long): Long = {
    (0L to skipTo).foldLeft(0L)((actuallySkipped, j) => {
      //println(actuallySkipped)
      if (actuallySkipped >= skipTo) return actuallySkipped
      try
        actuallySkipped + stream.skip(skipTo - actuallySkipped)
      catch {
        case e: IOException => throw new IOException(EMPTY_INPUT_STREAM)
      }
    })
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
  def getBytesFromFile(page: Seq[Byte], offset: Seq[Long], length: Seq[Int]): Seq[Seq[Byte]] = {
    offset.iterator.map(i => {
      val curOffset = offset(i.toInt).toInt

      if (page.length < curOffset) throw new IOException(EMPTY_INPUT_STREAM)
      page.slice(curOffset, curOffset + length(i.toInt))
    }).toSeq
  }

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
  def getBytesFromFile(fileStream: InputStream, position: Long, offset: Seq[Long], length: Seq[Int], rewind: Boolean = true): BytesReadResult = {

    if (rewind)
      fileStream.mark(Int.MaxValue)

    val res = (0L to offset.length - 1).foldLeft(BytesReadResult(relPosition = position))((acc: BytesReadResult, i: Long) => {
      skipStream(fileStream, offset(i.toInt) - acc.relPosition)
      // println(acc.lastPosition)
      val temp = new Array[Byte](length(i.toInt))

      val eof: Boolean = {
        try {
          fileStream.read(temp, 0, length(i.toInt))
          false
        }
        catch {
          case _: EOFException => true
        }
      }

      val lastRelPosition = offset(i.toInt) + length(i.toInt).toLong

      BytesReadResult(eof, acc.relPosition + lastRelPosition, lastRelPosition, acc.readBytes :+ temp.toSeq)
    })

    if (rewind)
      fileStream.reset()

    res
  }

  /**
    * The method to validate sas7bdat file. If sasFileProperties contains an encoding value other than
    * {@link SasFileConstants#LITTLE_ENDIAN_CHECKER} or {@link SasFileConstants#BIG_ENDIAN_CHECKER}
    * the file is considered invalid.
    *
    * @return true if the value of encoding equals to { @link SasFileConstants#LITTLE_ENDIAN_CHECKER}
    *                                                         or { @link SasFileConstants#BIG_ENDIAN_CHECKER}
    */
  def isSasFileValid(endianness: Int): Boolean =
    endianness == LittleEndianChecker || endianness == BigEndianChecker

  /**
    * The method to read and parse metadata from the sas7bdat file`s header in {@link SasFileParser#sasFileProperties}.
    * After reading is complete, {@link SasFileParser#currentFilePosition} is set to the end of the header whose length
    * is stored at the {@link SasFileConstants#HEADER_SIZE_OFFSET} offset.
    *
    * @param offset the initial sas file properties
    * @throws IOException if reading from the {@link SasFileParser#sasFileStream} stream is impossible.
    **/
  @throws[IOException]
  def readSasFileHeader(fileStream: InputStream): ParseResult = {

    val offsetForAlign = Seq(Align1Offset, Align2Offset)
    val lengthForAlign = Seq(Align1Length, Align2Length)
    val resU64 = getBytesFromFile(fileStream, 0, offsetForAlign, lengthForAlign, false)

    val isU64: Boolean = resU64.readBytes(0)(0) == U64ByteCheckerValue
    val align1 = if (resU64.readBytes(1)(0) == Align1CheckerValue) Align1Value else 0
    val align2 = if (isU64) Align2Value else 0
    val totalAlign = align1 + align2

    val offset = Seq(EndiannessOffset, EncodingOffset, DatasetOffset, FileTypeOffset,
      DateCreatedOffset + align1, DateModifiedOffset + align1, HeaderSizeOffset + align1,
      PageSizeOffset + align1, PageCountOffset + align1, SasReleaseOffset + totalAlign,
      SasServerTypeOffset + totalAlign, OsVersionNumberOffset + totalAlign,
      OsMarkerOffset + totalAlign, OsNameOffset + totalAlign)

    val length = Seq(EndiannessLength, EncodingLength, DatasetLength,
      FileTypeLength, DateCreatedLength, DateModifiedLength,
      HeaderSizeLength, PageSizeLength, PageCountLength + align2,
      SasReleaseLength, SasServerTypeLength, OsVersionNumberLength,
      OsMarkerLength, OsNameLength)

    val res = getBytesFromFile(fileStream, resU64.relPosition, offset, length, false)

    val endianeness = res.readBytes(0)(0)

    if (!isSasFileValid(endianeness))
      throw new IOException(FILE_NOT_VALID)

    val isEncodingPresent = SasCharacterEncodings.contains(res.readBytes(1)(0))
    val encoding = if (isEncodingPresent) SasCharacterEncodings(res.readBytes(1)(0)) else DefaultEncoding

    val properties = SasFileProperties(
      isU64, None, endianeness, encoding, null, bytesToString(res.readBytes(2),encoding).trim,
      bytesToString(res.readBytes(3),encoding).trim, bytesToDateTime(res.readBytes(4), endianeness),
      bytesToDateTime(res.readBytes(5), endianeness), bytesToString(res.readBytes(9),encoding).trim,
      bytesToString(res.readBytes(10),encoding).trim,
      if (res.readBytes(13)(0) != 0)
        bytesToString(res.readBytes(13),encoding).trim
      else
        bytesToString(res.readBytes(12),encoding).trim,
      bytesToString(res.readBytes(11),encoding).trim, bytesToInt(res.readBytes(6),endianeness),
      bytesToInt(res.readBytes(7),endianeness), bytesToLong(res.readBytes(8),isU64, endianeness)
    )

    //Move the file stream to the end of the header
    if (fileStream != null)
      skipStream(fileStream, properties.headerLength - res.relPosition)

    ParseResult(properties, properties.headerLength)
  }

  /**
    * The method to read page metadata and store it in {@link SasFileParser#currentPageType},
    * {@link SasFileParser#currentPageBlockCount} and {@link SasFileParser#currentPageSubheadersCount}.
    *
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} string is impossible.
    */
  @throws[IOException]
  def readPageHeader(fileStream: InputStream, properties: SasFileProperties): PageHeader = {
    val bitOffset = if (properties.isU64) PageBitOffsetX64 else PageBitOffsetX86
    val offset = Seq(bitOffset + PAGE_TYPE_OFFSET, bitOffset + BLOCK_COUNT_OFFSET, bitOffset + SUBHEADER_COUNT_OFFSET)
    val length = Seq(PAGE_TYPE_LENGTH, BLOCK_COUNT_LENGTH, SUBHEADER_COUNT_LENGTH)
    val vars = getBytesFromFile(fileStream, 0, offset, length)

    val pageType = bytesToShort(vars.readBytes(0), properties.endianness)
    val pageBlockCount = bytesToShort(vars.readBytes(1), properties.endianness)
    val pageSubheadersCount = bytesToShort(vars.readBytes(2), properties.endianness)

    logger.debug(PageType.format(pageType))
    logger.debug(BlockCount.format(pageBlockCount))
    logger.debug(SubheaderCount.format(pageSubheadersCount))

    PageHeader(pageType, pageBlockCount, pageSubheadersCount)
  }

  private def getBitOffset(properties: SasFileProperties): Int =
    if (properties.isU64) PageBitOffsetX64 else PageBitOffsetX86

  /**
    * The method to read pages of the sas7bdat file. First, the method reads the page type
    * (at the {@link SasFileConstants#PageTypeOffset} offset), the number of rows on the page
    * (at the {@link SasFileConstants#BlockCountOffset} offset), and the number of subheaders
    * (at the {@link SasFileConstants#SubheaderCountOffset} offset). Then, depending on the page type,
    * the method calls the function to process the page.
    *
    * @return true if all metadata is read.
    * @throws IOException if reading from the {@link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def processSasFilePageMeta(fileStream: InputStream, header: PageHeader, properties: SasFileProperties) = {
    //val subheaderPointers = new util.ArrayList[Nothing]

    if ((header.pageType == PAGE_META_TYPE_1) || (header.pageType == PAGE_META_TYPE_2) || (header.pageType == PAGE_MIX_TYPE))
      readPageMetadata(fileStream, header, properties)

    //(currentPageType eq PAGE_DATA_TYPE) || (currentPageType eq PAGE_MIX_TYPE) || (currentPageDataSubheaderPointers.size ne 0)
  }

  /**
    * The method to parse and read metadata of a page, used for pages of the {@link SasFileConstants#PAGE_META_TYPE_1}
    * {@link SasFileConstants#PAGE_META_TYPE_1}, and {@link SasFileConstants#PAGE_MIX_TYPE} types. The method goes
    * through subheaders, one by one, and calls the processing functions depending on their signatures.
    *
    * @param bitOffset         the offset from the beginning of the page at which the page stores its metadata.
    * @param subheaderPointers the number of subheaders on the page.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} string is impossible.
    */
  @throws[IOException]
  def readPageMetadata(fileStream: InputStream, header: PageHeader, properties: SasFileProperties): MetadataReadResult = {

    val pointers = (0 to header.subheaderCount - 1).map(i => {

      val subheaderPointer = readSubheaderPointer(fileStream, properties, i)

      if (subheaderPointer.compression != TruncatedSubheaderId) {
        val subheaderSignature = readSubheaderSignature(fileStream, properties, subheaderPointer.offset)
        val subheaderIndex = chooseSubheaderClass(properties, subheaderSignature, subheaderPointer.compression, subheaderPointer._type)

        val newProperties: SasFileProperties = {
          if (subheaderIndex != null && subheaderIndex != SubheaderIndexes.DataSubheaderIndex) {
              logger.debug(SubheaderProcessFunctionName, subheaderIndex)
              subheaderIndexToClass(SubheaderIndexes(i)).processSubheader(fileStream, properties, subheaderPointer.offset, subheaderPointer.length)
          }
          else
            properties
        }
        (subheaderPointer, newProperties)
      }
      else {
        logger.debug(UnknownSubheaderSignature)
        (subheaderPointer, properties)
      }
    })

    MetadataReadResult(pointers)
    /*setCompressionMethod
    setColumnsCount
    setRowLength
    setRowCount
    setMixPageRowCount*/
    //val newProperties = pointers.flatMap(_._2).
    //  foldLeft(properties)((oldProperties, newProperties) => oldProperties.)
  }

  /**
    * The function to read a subheader signature at the offset known from its ({@link SubheaderPointer}).
    *
    * @param subheaderPointerOffset the offset at which the subheader is located.
    * @return - the subheader signature to search for in the {@link SasFileParser#SubheaderSignatureToIndex} mapping later.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def readSubheaderSignature(fileStream: InputStream, properties: SasFileProperties, pointerOffset: Long): Long = {
    val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
    val offsetMass = Seq(pointerOffset)
    val lengthMass = Seq(intOrLongLength)

    val res = getBytesFromFile(fileStream, 0, offsetMass, lengthMass)

    bytesToLong(res.readBytes(0), properties.isU64, properties.endianness)
  }

  /**
    * The function to determine the subheader type by its signature, {@link SubheaderPointer#compression},
    * and {@link SubheaderPointer#type}.
    *
    * @param signature the subheader signature to search for in the {@link SasFileParser#SubheaderSignatureToIndex} mapping
    * @param compression the type of subheader compression ({ @link SubheaderPointer#compression})
    * @param type the subheader type ({@link SubheaderPointer#_type})
    * @return an element from the  {@link SubheaderIndexes} enumeration that defines the type of the current subheader
    */
  def chooseSubheaderClass(properties: SasFileProperties, signature: Long, compression: Int, _type: Int): SubheaderIndexes = {
    if (properties.isCompressed && SubheaderSignatureToIndex.contains(signature) &&
      (compression == COMPRESSED_SUBHEADER_ID || compression == 0) && _type == COMPRESSED_SUBHEADER_TYPE)
      SubheaderIndexes.DataSubheaderIndex
    else
      SubheaderSignatureToIndex(signature)
  }

  /**
      * The function to read the pointer with the subheaderPointerIndex index from the list of {@link SubheaderPointer}
      * located at the subheaderPointerOffset offset.
      *
      * @param pointerOffset the offset before the list of {@link SubheaderPointer}.
      * @param pointerIndex the index of the subheader pointer being read.
      * @return the subheader pointer.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
  @throws[IOException]
  def readSubheaderPointer(fileStream: InputStream, properties: SasFileProperties, pointerIndex: Int)
                          (implicit pointerOffset: Long = getBitOffset(properties).toLong + SubheaderPointersOffset): SubheaderPointer = {

      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val subheaderPointerLength = if (properties.isU64) SubheaderPointerLengthX64 else SubheaderPointerLengthX86
      val totalOffset = pointerOffset + subheaderPointerLength * pointerIndex.toLong

      val offset = Seq(totalOffset, totalOffset + intOrLongLength, totalOffset + 2L * intOrLongLength, totalOffset + 2L * intOrLongLength + 1)
      val length = Seq(intOrLongLength, intOrLongLength, 1, 1)

      val vars = getBytesFromFile(fileStream, 0, offset, length).readBytes

      val subheaderOffset = bytesToLong(vars(0), properties.isU64, properties.endianness)
      val subheaderLength = bytesToLong(vars(1), properties.isU64, properties.endianness)
      val subheaderCompression = vars(2)(0)
      val subheaderType = vars(3)(0)

      SubheaderPointer(subheaderOffset, subheaderLength, subheaderCompression, subheaderType)
    }

  /**
    * Return the compression literal if it is contained in the input string.
    * If the are many the first match is return.
    * If there are no matches, <code>None</code> is returned
    *
    * @param src input string to look for matches
    * @return First match of a supported compression literal or null if no literal matches the input string.
    */
  private def findCompressionLiteral(src: String): Option[String] = {
    src match {
      case null => {
        logger.warn(NullCompressionLiteral)
        None
      }
      case _ => {
        LiteralsToDecompressor.keySet.foreach(compressor =>
          if (src.contains(compressor))
            return Some(compressor)
        )
        logger.debug(NoSupportedCompressionLiteral)
        None
      }
    }
  }
}

