package com.kaszub.parso.impl

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.kaszub.parso.DataWriterUtil.EMPTY_INPUT_STREAM
import com.kaszub.parso.{Column, ColumnMissingInfo, DataWriterUtil, SasFileProperties}
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
    * The mapping of subheader signatures to the corresponding elements in {@link SubheaderIndexes}.
    * Depending on the value at the {@link SasFileConstants#ALIGN_2_OFFSET} offset, signatures take 4 bytes
    * for 32-bit version sas7bdat files and 8 bytes for the 64-bit version files.
    */
  private val SUBHEADER_SIGNATURE_TO_INDEX = Map[Long, SubheaderIndexes.Value](
    0xF7F7F7F7.toLong -> SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX,
    0xF6F6F6F6.toLong -> SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX,
    0xFFFFFC00.toLong -> SubheaderIndexes.SUBHEADER_COUNTS_SUBHEADER_INDEX,
    0xFFFFFFFD.toLong -> SubheaderIndexes.COLUMN_TEXT_SUBHEADER_INDEX,
    0xFFFFFFFF.toLong -> SubheaderIndexes.COLUMN_NAME_SUBHEADER_INDEX,
    0xFFFFFFFC.toLong -> SubheaderIndexes.COLUMN_ATTRIBUTES_SUBHEADER_INDEX,
    0xFFFFFBFE.toLong -> SubheaderIndexes.FORMAT_AND_LABEL_SUBHEADER_INDEX,
    0xFFFFFFFE.toLong -> SubheaderIndexes.COLUMN_LIST_SUBHEADER_INDEX,
    0x00000000F7F7F7F7L -> SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX,
    0x00000000F6F6F6F6L -> SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX,
    0xF7F7F7F700000000L -> SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX,
    0xF6F6F6F600000000L -> SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX,
    0xF7F7F7F7FFFFFBFEL -> SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX,
    0xF6F6F6F6FFFFFBFEL -> SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX,
    0x00FCFFFFFFFFFFFFL -> SubheaderIndexes.SUBHEADER_COUNTS_SUBHEADER_INDEX,
    0xFDFFFFFFFFFFFFFFL -> SubheaderIndexes.COLUMN_TEXT_SUBHEADER_INDEX,
    0xFFFFFFFFFFFFFFFFL -> SubheaderIndexes.COLUMN_NAME_SUBHEADER_INDEX,
    0xFCFFFFFFFFFFFFFFL -> SubheaderIndexes.COLUMN_ATTRIBUTES_SUBHEADER_INDEX,
    0xFEFBFFFFFFFFFFFFL -> SubheaderIndexes.FORMAT_AND_LABEL_SUBHEADER_INDEX,
    0xFEFFFFFFFFFFFFFFL -> SubheaderIndexes.COLUMN_LIST_SUBHEADER_INDEX
  )

  /**
    * The mapping of the supported string literals to the compression method they mean.
    */
  private val LITERALS_TO_DECOMPRESSOR = Map[String, Decompressor](
    COMPRESS_CHAR_IDENTIFYING_STRING -> CharDecompressor,
    //COMPRESS_BIN_IDENTIFYING_STRING -> BinDecompressor
  )

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

  /**
    * The mapping between elements from {@link SubheaderIndexes} and classes corresponding
    * to each subheader. This is necessary because defining the subheader type being processed is dynamic.
    * Every class has an overridden function that processes the related subheader type.
    */
  private val subheaderIndexToClass = Map[SubheaderIndexes.Value, ProcessingSubheader](
    SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX -> RowSizeSubheader(sasFileProperties),
    SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX -> ColumnSizeSubheader(sasFileProperties),
    SubheaderIndexes.SUBHEADER_COUNTS_SUBHEADER_INDEX -> SubheaderCountsSubheader(),
    SubheaderIndexes.COLUMN_TEXT_SUBHEADER_INDEX -> ColumnTextSubheader(sasFileProperties),
    /*SubheaderIndexes.COLUMN_NAME_SUBHEADER_INDEX -> new ColumnNameSubheader,
    SubheaderIndexes.COLUMN_ATTRIBUTES_SUBHEADER_INDEX -> new ColumnAttributesSubheader,
    SubheaderIndexes.FORMAT_AND_LABEL_SUBHEADER_INDEX -> new FormatAndLabelSubheader,
    SubheaderIndexes.COLUMN_LIST_SUBHEADER_INDEX -> new ColumnListSubheader,
    SubheaderIndexes.DATA_SUBHEADER_INDEX -> new DataSubheader*/
  )

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
    val bitOffset = if (sasFileProperties.isU64) PAGE_BIT_OFFSET_X64 else PAGE_BIT_OFFSET_X86
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
    * Enumeration of missing information types.
    */
  object SubheaderIndexes extends Enumeration {
    type SubheaderIndexes = Value
    /**
      * Index which define row size subheader, which contains rows size in bytes and the number of rows.
      */
    val ROW_SIZE_SUBHEADER_INDEX = Value

    /**
      * Index which define column size subheader, which contains columns count.
      */
    val COLUMN_SIZE_SUBHEADER_INDEX = Value

    /**
      * Index which define subheader counts subheader, which contains currently not used data.
      */
    val SUBHEADER_COUNTS_SUBHEADER_INDEX = Value

    /**
      * Index which define column text subheader, which contains type of file compression
      * and info about columns (name, label, format).
      */
    val COLUMN_TEXT_SUBHEADER_INDEX = Value

    /**
      * Index which define column name subheader, which contains column names.
      */
    val COLUMN_NAME_SUBHEADER_INDEX = Value

    /**
      * Index which define column attributes subheader, which contains column attributes, such as type.
      */
    val COLUMN_ATTRIBUTES_SUBHEADER_INDEX = Value

    /**
      * Index which define format and label subheader, which contains info about format of objects in column
      * and tooltip text for columns.
      */
    val FORMAT_AND_LABEL_SUBHEADER_INDEX = Value

    /**
      * Index which define column list subheader, which contains currently not used data.
      */
    val COLUMN_LIST_SUBHEADER_INDEX = Value

    /**
      * Index which define data subheader, which contains sas7bdat file rows data.
      */
    val DATA_SUBHEADER_INDEX = Value
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
    def processSubheader(subheaderOffset: Long, subheaderLength: Long): Unit
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

  /**
    * The class to process subheaders of the RowSizeSubheader type that store information about the table rows length
    * (in bytes), the number of rows in the table and the number of rows on the last page of the
    * {@link SasFileConstants#PAGE_MIX_TYPE} type. The results are stored in {@link SasFileProperties#rowLength},
    * {@link SasFileProperties#rowCount}, and {@link SasFileProperties#mixPageRowCount}, respectively.
    */
  case class RowSizeSubheader(properties: SasFileProperties) extends ProcessingSubheader {
    /**
      * The function to read the following metadata about rows of the sas7bdat file:
      * {@link SasFileProperties#rowLength}, {@link SasFileProperties#rowCount},
      * and {@link SasFileProperties#mixPageRowCount}.
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    @throws[IOException]
    def processSubheader(subheaderOffset: Long, subheaderLength: Long): Unit = {
      val intOrLongLength = if (sasFileProperties.isU64) BYTES_IN_LONG else BYTES_IN_INT
      val offset = Seq(
        subheaderOffset + ROW_LENGTH_OFFSET_MULTIPLIER * intOrLongLength,
        subheaderOffset + ROW_COUNT_OFFSET_MULTIPLIER * intOrLongLength,
        subheaderOffset + ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER * intOrLongLength)
      val length = Seq(intOrLongLength, intOrLongLength, intOrLongLength)
      val vars = getBytesFromFile(offset, length)

      val isU64 = properties.isU64
      val endianness = properties.endianness

      val propertiesRowLength =
        if (sasFileProperties.rowLength == 0)
          properties.setRowLength(SasFileParser.bytesToLong(vars(0), isU64, endianness))
        else
          properties

      val propertiesRowCount =
        if (sasFileProperties.rowCount == 0)
          propertiesRowLength.setRowCount(SasFileParser.bytesToLong(vars(1), isU64, endianness))
        else
          propertiesRowLength

      val propertiesMixPageRowCount =
        if (sasFileProperties.mixPageRowCount == 0)
          propertiesRowCount.setMixPageRowCount(SasFileParser.bytesToLong(vars(2), isU64, endianness))
        else
          propertiesRowCount

      //TODO: Remove this
      sasFileProperties = propertiesMixPageRowCount
    }
  }

  /**
    * The class to process subheaders of the ColumnSizeSubheader type that store information about
    * the number of table columns. The {@link SasFileProperties#columnsCount} variable stores the results.
    */
  case class ColumnSizeSubheader(properties: SasFileProperties) extends ProcessingSubheader {
    /**
      * The function to read the following metadata about columns of the sas7bdat file:
      * {@link SasFileProperties#columnsCount}.
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    @throws[IOException]
    def processSubheader(subheaderOffset: Long, subheaderLength: Long): Unit = {
      val intOrLongLength = if (sasFileProperties.isU64) BYTES_IN_LONG else BYTES_IN_INT
      val offset = Seq(subheaderOffset + intOrLongLength)
      val length = Seq(intOrLongLength)
      val vars = getBytesFromFile(offset, length)

      //TODO: Remove this
      sasFileProperties =
        properties.setColumnsCount(SasFileParser.bytesToLong(vars(0), sasFileProperties.isU64,sasFileProperties.endianness))
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
      def processSubheader(subheaderOffset: Long, subheaderLength: Long): Unit = {}
    }

    /**
      * The class to process subheaders of the ColumnTextSubheader type that store information about
      * file compression and table columns (name, label, format). The first subheader of this type includes the file
      * compression information. The results are stored in {@link SasFileParser#columnsNamesBytes} and
      * {@link SasFileProperties#compressionMethod}.
      */
    case class ColumnTextSubheader(properties: SasFileProperties) extends ProcessingSubheader {
      /**
        * The function to read the text block with information about file compression and table columns (name, label,
        * format) from a subheader. The first text block of this type includes the file compression information.
        *
        * @param subheaderOffset the offset at which the subheader is located.
        * @param subheaderLength the subheader length.
        * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
        */
      def processSubheader(subheaderOffset: Long, subheaderLength: Long): Unit = {
        val intOrLongLength = if (sasFileProperties.isU64) BYTES_IN_LONG else BYTES_IN_INT
        val offset = Seq(subheaderOffset + intOrLongLength)
        val lengthByteBuffer = Seq(TEXT_BLOCK_SIZE_LENGTH)

        val vars = getBytesFromFile(offset, lengthByteBuffer)
        val textBlockSize = SasFileParser.byteArrayToByteBuffer(vars(0), properties.endianness).getInt//.getShort

        val lengthFile = Seq(textBlockSize)
        val varsFile = getBytesFromFile(offset, lengthFile)

        //columnsNamesBytes.add(vars(0))

        //TODO: Remove this
        /*sasFileProperties = {
          if (columnsNamesBytes.size == 1) {
            val columnName = columnsNamesBytes(0)
            val compessionLiteral = findCompressionLiteral(bytesToString(columnName))
            properties.setCompressionMethod(compessionLiteral) //might be null
          }
          else
            properties
        }*/
        ???
      }
    }
}

object SasFileParser extends ParserMessageConstants with SasFileConstants {

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
        (doubleSeconds - START_DATES_SECONDS_DIFFERENCE).toLong).atOffset(ZoneOffset.UTC).toZonedDateTime
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
        ((doubleDays - START_DATES_DAYS_DIFFERENCE) *
          SECONDS_IN_MINUTE *
          MINUTES_IN_HOUR *
          HOURS_IN_DAY).toLong).atOffset(ZoneOffset.UTC).toZonedDateTime
  }

  /**
    * The function to convert an array of bytes into a double number.
    *
    * @param bytes a double number represented by an array of bytes.
    * @return a number of the double type that is the conversion result.
    */
  def bytesToDouble(bytes: Seq[Byte], endianness: Int): Double = {

    val original = byteArrayToByteBuffer(bytes, endianness)

    if (bytes.length < BYTES_IN_DOUBLE) {
      val byteBuffer = ByteBuffer.allocate(BYTES_IN_DOUBLE)

      if (endianness == LITTLE_ENDIAN_CHECKER)
        byteBuffer.position(BYTES_IN_DOUBLE - bytes.length)

      byteBuffer.put(original)
      byteBuffer.order(original.order)
      byteBuffer.position(0)
      byteBuffer.getDouble
    }
    else
      original.getDouble
  }

  /**
    * The result of reading bytes from a file
    *
    * @param eof          whether the eof file was reached while reading the stream
    * @param lastPosition the last position when reading the input stream
    * @param readBytes    the matrix of read bytes
    */
  case class BytesReadResult(eof: Boolean = false, lastPosition: Long = 0L, readBytes: Seq[Seq[Byte]] = Seq())

  private def skipStream(stream: InputStream, skipTo: Long): Long = {
    (0L to skipTo).foldLeft(0L)((actuallySkipped, j) => {
      //println(j)
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
    * @return the list of bytes arrays.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def getBytesFromFile(fileStream: BufferedInputStream, position: Long, offset: Seq[Long], length: Seq[Int]): BytesReadResult = {

    (0L to offset.length - 1).foldLeft(BytesReadResult(lastPosition = position))((acc: BytesReadResult, i: Long) => {
      skipStream(fileStream, offset(i.toInt) - acc.lastPosition)
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

      BytesReadResult(eof, offset(i.toInt) + length(i.toInt).toLong, acc.readBytes :+ temp.toSeq)
    })
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
    endianness == LITTLE_ENDIAN_CHECKER || endianness == BIG_ENDIAN_CHECKER

  /**
    * The method to read and parse metadata from the sas7bdat file`s header in {@link SasFileParser#sasFileProperties}.
    * After reading is complete, {@link SasFileParser#currentFilePosition} is set to the end of the header whose length
    * is stored at the {@link SasFileConstants#HEADER_SIZE_OFFSET} offset.
    *
    * @param offset the initial sas file properties
    * @throws IOException if reading from the {@link SasFileParser#sasFileStream} stream is impossible.
    **/
  @throws[IOException]
  def processSasFileHeader(fileStream: BufferedInputStream): SasFileProperties = {

    val offsetForAlign = Seq(ALIGN_1_OFFSET, ALIGN_2_OFFSET)
    val lengthForAlign = Seq(ALIGN_1_LENGTH, ALIGN_2_LENGTH)
    val resU64 = getBytesFromFile(fileStream, 0, offsetForAlign, lengthForAlign)

    val isU64: Boolean = resU64.readBytes(0)(0) == U64_BYTE_CHECKER_VALUE
    val align1 = if (resU64.readBytes(1)(0) == ALIGN_1_CHECKER_VALUE) ALIGN_1_VALUE else 0
    val align2 = if (isU64) ALIGN_2_VALUE else 0
    val totalAlign = align1 + align2

    val offset = Seq(ENDIANNESS_OFFSET, ENCODING_OFFSET, DATASET_OFFSET, FILE_TYPE_OFFSET,
      DATE_CREATED_OFFSET + align1, DATE_MODIFIED_OFFSET + align1, HEADER_SIZE_OFFSET + align1,
      PAGE_SIZE_OFFSET + align1, PAGE_COUNT_OFFSET + align1, SAS_RELEASE_OFFSET + totalAlign,
      SAS_SERVER_TYPE_OFFSET + totalAlign, OS_VERSION_NUMBER_OFFSET + totalAlign,
      OS_MAKER_OFFSET + totalAlign, OS_NAME_OFFSET + totalAlign)

    val length = Seq(ENDIANNESS_LENGTH, ENCODING_LENGTH, DATASET_LENGTH,
      FILE_TYPE_LENGTH, DATE_CREATED_LENGTH, DATE_MODIFIED_LENGTH,
      HEADER_SIZE_LENGTH, PAGE_SIZE_LENGTH, PAGE_COUNT_LENGTH + align2,
      SAS_RELEASE_LENGTH, SAS_SERVER_TYPE_LENGTH, OS_VERSION_NUMBER_LENGTH,
      OS_MAKER_LENGTH, OS_NAME_LENGTH)

    val res = getBytesFromFile(fileStream, resU64.lastPosition, offset, length)

    val endianeness = res.readBytes(0)(0)

    if (!isSasFileValid(endianeness))
      throw new IOException(FILE_NOT_VALID)

    val isEncodingPresent = SAS_CHARACTER_ENCODINGS.contains(res.readBytes(1)(0))
    val encoding = if (isEncodingPresent) SAS_CHARACTER_ENCODINGS(res.readBytes(1)(0)) else DefaultEncoding

    val properties = SasFileProperties(
      isU64, null, endianeness, encoding, null, bytesToString(res.readBytes(2),encoding).trim,
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

    //Skip the rest of the bytes in the header length
    if (fileStream != null)
      skipStream(fileStream, properties.headerLength - res.lastPosition)
    //currentFilePosition = 0

    properties

  }

}

