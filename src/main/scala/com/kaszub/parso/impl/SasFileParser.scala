package com.kaszub.parso.impl

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.kaszub.parso.impl.SasFileParser.SubheaderIndexes.SubheaderIndexes
import com.kaszub.parso._
import com.kaszub.parso.impl.SasFileParser.readNextPage
import com.sun.net.httpserver.Authenticator
import com.typesafe.scalalogging.Logger

import scala.collection.parallel.CollectionConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class SasFileParser private(val sasFileStream: BufferedInputStream = null,
                                 encoding: String = null,
                                 byteOutput: Boolean = false
                                ) extends SasFileConstants with ParserMessageConstants {

  private def this(builder: SasFileParser#Builder) = {
    this(new BufferedInputStream(builder.inputStream), builder.encoding, builder.byteOutput)
  }

  /**
    * SasFileParser builder class made using builder pattern.
    */
  case class Builder(inputStream: InputStream = null, encoding: String = "US-ASCII", byteOutput: Boolean = false) {

    /**
      * The function to specify builders sasFileStream variable.
      *
      * @param val value to be set.
      * @return result builder.
      */
    def sasFileStream(value: InputStream): Builder = Builder(value)

    /**
      * The function to specify builders encoding variable.
      *
      * @param val value to be set.
      * @return result builder.
      */
    def encoding(value: String): Builder = Builder(inputStream, value)

    /**
      * The function to specify builders byteOutput variable.
      *
      * @param val value to be set.
      * @return result builder.
      */
    def byteOutput(value: Boolean): Builder = Builder(inputStream, encoding, value)

    /**
      * The function to create variable of SasFileParser class using current builder.
      *
      * @return newly built SasFileParser
      */
    def build: SasFileParser = new SasFileParser(this)
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
    CompressCharIdentifyingString -> CharDecompressor,
    CompressBinIdentifyingString -> BinDecompressor
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
    SubheaderIndexes.FormatAndLabelSubheaderIndex -> FormatAndLabelSubheader(),
    SubheaderIndexes.ColumnListSubheaderIndex -> ColumnListSubheader(),
    SubheaderIndexes.DataSubheaderIndex -> DataSubheader()
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
    * @return { @link ByteBuffer} with the order of bytes defined in the file located at
    *                 the { @link SasFileConstants#ALIGN_2_OFFSET} offset.
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
  private def convertByteArrayToNumber(mass: Seq[Byte], endianness: Int): Any = {
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
    * The function to remove excess symbols from the end of a bytes array. Excess symbols are line end characters,
    * tabulation characters, and spaces, which do not contain useful information.
    *
    * @param source an array of bytes containing required data.
    * @param offset the offset in source of required data.
    * @param length the length of required data.
    * @return the array of bytes without excess symbols at the end.
    */
  private def trimBytesArray(source: Seq[Byte], offset: Int, length: Int): Seq[Byte] = {
    val lengthFromBegin =
      (offset + length until offset by -1).filter(i => source(i - 1) != ' ' && source(i - 1) != '\u0000' && source(i - 1) != '\t').head

    if (lengthFromBegin - offset != 0)
      source.slice(offset, lengthFromBegin)
    else
      Seq()
  }

  /**
    * The class to store subheaders pointers that contain information about the offset, length, type
    * and compression of subheaders (see {@link SasFileConstants#TruncatedSubheaderId},
    * {@link SasFileConstants#CompressedSubheaderId}, {@link SasFileConstants#CompressedSubheaderType}
    * for details).
    *
    * @param offset      The offset from the beginning of a page at which a subheader is stored.
    * @param length      The subheader length
    * @param compression The type of subheader compression. If the type is { @link SasFileConstants#TruncatedSubheaderId}
    *                                                                              the subheader does not contain information relevant to the current issues. If the type is
    *                                                                              { @link SasFileConstants#CompressedSubheaderId} the subheader can be compressed (depends on { @link SubheaderPointer#_type})
    * @param _type The subheader type. If the type is { @link SasFileConstants#CompressedSubheaderType} the subheader
    *                                                         is compressed. Otherwise, there is no compression.
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean = true): SasFileProperties
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val offsetSubheader = Seq(
        offset + ROW_LENGTH_OFFSET_MULTIPLIER * intOrLongLength,
        offset + ROW_COUNT_OFFSET_MULTIPLIER * intOrLongLength,
        offset + ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER * intOrLongLength)
      val offsetLength = Seq(intOrLongLength, intOrLongLength, intOrLongLength)

      val vars = inputStream.readBytes(0, offsetSubheader, offsetLength).readBytes

      val isU64 = properties.isU64
      val endianness = properties.endianness

      val rowLen = SasFileParser.bytesToLong(vars(0), isU64, endianness)

      val propertiesRowLength =
        if (properties.rowLength == 0)
          if (copy) properties.setRowLength(rowLen) else SasFileProperties(rowLength = rowLen)
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val offsetSubheader = Seq(offset + intOrLongLength)
      val lengthSubheader = Seq(intOrLongLength)

      val vars = inputStream.readBytes(0, offsetSubheader, lengthSubheader).readBytes
      val colCount = bytesToLong(vars(0), properties.isU64, properties.endianness)

      if (copy)
        properties.setColumnsCount(colCount)
      else
        SasFileProperties(columnsCount = colCount)
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
      * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
      */
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = properties
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val offsetSubheader = Seq(offset + intOrLongLength)
      val lengthByteBuffer = Seq(TextBlockSizeLength)

      val vars = inputStream.readBytes(0, offsetSubheader, lengthByteBuffer).readBytes
      val textBlockSize = byteArrayToByteBuffer(vars(0), properties.endianness).getShort

      val lengthFile = Seq(textBlockSize.toInt)
      val varsFile = inputStream.readBytes(0, offsetSubheader, lengthFile).readBytes

      val columnName = varsFile(0)
      val compression = findCompressionLiteral(bytesToString(columnName, properties.encoding))

      val newProperties = {
        if (copy)
          properties.setCompressionMethod(compression)
        else
          SasFileProperties(compressionMethod = compression)
      }

      newProperties.setColumnsNamesBytes(Seq(columnName))
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val columnNamePointersCount = (length - 2 * intOrLongLength - 12) / 8

      val colNames = (0 until columnNamePointersCount.toInt).map(i => {
        val colOffset = Seq(
          offset + intOrLongLength + ColumnNamePointerLength * (i + 1) + ColumnNameTextSubheaderOffset,
          offset + intOrLongLength + ColumnNamePointerLength * (i + 1) + ColumnNameOffsetOffset,
          offset + intOrLongLength + ColumnNamePointerLength * (i + 1) + ColumnNameLengthOffset)
        val colLength = Seq(ColumnNameTextSubheaderLength, ColumnNameOffsetLength, ColumnNameLengthLength)

        val vars = inputStream.readBytes(0, colOffset, colLength).readBytes

        val textSubheaderIndex = bytesToShort(vars(0), properties.endianness)
        val columnNameOffset = bytesToShort(vars(1), properties.endianness)
        val columnNameLength = bytesToShort(vars(2), properties.endianness)

        if (textSubheaderIndex < properties.columnsNamesBytes.size)
          Left(
            bytesToString(properties.columnsNamesBytes(textSubheaderIndex), columnNameOffset, columnNameLength, properties.encoding).intern())
        else
          Right(
            ColumnMissingInfo(i, textSubheaderIndex, columnNameOffset, columnNameLength, MissingInfoType.NAME))
      })

      if (copy) properties.setColumnsNames(colNames) else SasFileProperties(columnNames = colNames)
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val columnAttributesVectorsCount = (length - 2 * intOrLongLength - 12) / (intOrLongLength + 8)

      val attributes = (0 until columnAttributesVectorsCount.toInt).map(i => {
        val attOffset = Seq(
          offset + intOrLongLength + ColumnDataOffsetOffset + i * (intOrLongLength + 8),
          offset + 2 * intOrLongLength + ColumnDataLengthOffset + i * (intOrLongLength + 8),
          offset + 2 * intOrLongLength + ColumnTypeOffset + i * (intOrLongLength + 8))
        val attLength = Seq(intOrLongLength, ColumnDataLengthLength, ColumnTypeLength)

        val vars = inputStream.readBytes(0, attOffset, attLength).readBytes

        ColumnAttributes(
          bytesToLong(vars(0), properties.isU64, properties.endianness),
          bytesToInt(vars(1), properties.endianness),
          if (vars(2)(0) == 1) classOf[Number] else classOf[String]
        )
      })

      if (copy) properties.setColumnsAttributes(attributes) else SasFileProperties(columnAttributes = attributes)
    }
  }

  /**
    * The class to process subheaders of the FormatAndLabelSubheader type that store the following information:
    * - the index of the ColumnTextSubheader type subheader whose text field contains the column format,
    * - the index of the ColumnTextSubheader type whose text field stores the label of the column corresponding
    * to the current subheader,
    * - offsets (in symbols) of the formats and labels from the beginning of the text field,
    * - lengths of the formats and labels (in symbols),
    * The {@link SasFileParser#columns} list stores the results.
    */
  case class FormatAndLabelSubheader() extends ProcessingSubheader {
    /**
      * The function to read the following data from the subheader:
      * - the index that stores the format of the column corresponding to the current subheader,
      * - the offset (in symbols) of the format inside the text block,
      * - the format length (in symbols),
      * - the index that stores the label of the column corresponding to the current subheader,
      * - the offset (in symbols) of the label inside the text block,
      * - the label length (in symbols).
      *
      * @param subheaderOffset the offset at which the subheader is located.
      * @param subheaderLength the subheader length.
      */
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
      val formatOffset = Seq(
        offset + ColumnFormatWidthOffset + intOrLongLength,
        offset + ColumnFormatPrecisionOffset + intOrLongLength,
        offset + ColumnFormatTextSubheaderIndexOffset + 3 * intOrLongLength,
        offset + ColumnFormatOffsetOffset + 3 * intOrLongLength,
        offset + ColumnFormatLengthOffset + 3 * intOrLongLength,
        offset + ColumnLabelTextSubheaderIndexOffset + 3 * intOrLongLength,
        offset + ColumnLabelOffsetOffset + 3 * intOrLongLength,
        offset + ColumnLabelLengthOffset + 3 * intOrLongLength)
      val formatLength = Seq(ColumnFormatWidthOffsetLength, ColumnFormatPrecisionOffsetLength, ColumnFormatTextSubheaderIndexLength,
        ColumnFormatOffsetLength, ColumnFormatLengthLength, ColumnLabelTextSubheaderIndexLength, ColumnLabelOffsetLength, ColumnLabelLengthLength)

      val vars = inputStream.readBytes(0, formatOffset, formatLength).readBytes

      val columnFormatWidth = bytesToShort(vars(0), properties.endianness)
      val columnFormatPrecision = bytesToShort(vars(1), properties.endianness)
      val textSubheaderIndexForFormat = bytesToShort(vars(2), properties.endianness)
      val columnFormatOffset = bytesToShort(vars(3), properties.endianness)
      val columnFormatLength = bytesToShort(vars(4), properties.endianness)
      val textSubheaderIndexForLabel = bytesToShort(vars(5), properties.endianness)
      val columnLabelOffset = bytesToShort(vars(6), properties.endianness)
      val columnLabelLength = bytesToShort(vars(7), properties.endianness)

      val columnLabel: Either[String, ColumnMissingInfo] = {
        if (textSubheaderIndexForLabel < properties.columnsNamesBytes.size)
          Left(
            bytesToString(properties.columnsNamesBytes(textSubheaderIndexForLabel), columnLabelOffset, columnLabelLength, properties.encoding).intern)
        else
          Right(
            ColumnMissingInfo(0, textSubheaderIndexForLabel, columnLabelOffset, columnLabelLength, MissingInfoType.LABEL))
      }
      val columnFormatName: Either[String, ColumnMissingInfo] = {
        if (textSubheaderIndexForFormat < properties.columnsNamesBytes.size)
          Left(
            bytesToString(properties.columnsNamesBytes(textSubheaderIndexForFormat), columnFormatOffset, columnFormatLength, properties.encoding).intern)
        else
          Right(
            ColumnMissingInfo(0, textSubheaderIndexForFormat, columnFormatOffset, columnFormatLength, MissingInfoType.FORMAT))
      }

      logger.debug(ColumnFormatMsg.format(columnFormatName match { case Left(v) => v case _ => "FORMAT MISSING" }))

      val formats = Seq(ColumnFormat(columnFormatName, columnFormatWidth, columnFormatPrecision))
      val formatsProperties = if (copy) properties.setColumnFormats(formats) else SasFileProperties(columnFormats = formats)

      formatsProperties.setColumnLabels(Seq(ColumnLabel(columnLabel)))
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
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = properties
  }

  /**
    * The class to process subheaders of the DataSubheader type that keep compressed or uncompressed data.
    */
  case class DataSubheader() extends ProcessingSubheader {
    /**
      * The method to read compressed or uncompressed data from the subheader. The results are stored as a row
      * in {@link SasFileParser#currentRow}. The {@link SasFileParser#processByteArrayWithData(long, long, List)}
      * function converts the array of bytes into a list of objects.
      *
      * @param offset the offset at which the subheader is located.
      * @param length the subheader length.
      */
    def processSubheader(inputStream: SasPageReader, properties: SasFileProperties, offset: Long, length: Long, copy: Boolean): SasFileProperties = {
      val page = inputStream.readBytes(0, Seq(offset), Seq(length.toInt)).readBytes
      val curRow = processByteArrayWithData(page(0), properties, 0, length)

      if (copy) properties.setRow(curRow) else SasFileProperties(row = curRow)
    }
  }

  case class MetadataReadResult(success: Boolean, properties: SasFileProperties)

  case class SasMetadata(pageHeader: PageHeader, properties: SasFileProperties) {
    def getPageRows: Int = {
      pageHeader.pageType match {
        case PageMetaType1 | PageMetaType2 => properties.dataSubheaderPointers.size
        case PageMixType => Math.min(properties.rowCount, properties.mixPageRowCount).toInt
        case PageDataType => pageHeader.blockCount
      }
    }

  }

  case class PageMetadataReadResult(subheaderIndex: Option[SubheaderIndexes], pointer: SubheaderPointer, properties: SasFileProperties)

  case class PageRowReadResult(row: Seq[Option[Any]], lastRow: Boolean)

  /**
    * The result of parsing a SAS file
    *
    * @param properties  the sas file properties after parsing
    * @param relPosition the last relative position when reading the input stream
    */
  //todo: move position to a separate class?
  case class ParseResult(properties: SasFileProperties = null, absPosition: Long = 0L)

  case class PageHeader(pageType: Int = 0, blockCount: Int = 0, subheaderCount: Int = 0, eof: Boolean = false)

  /**
    * The method to validate sas7bdat file. If sasFileProperties contains an encoding value other than
    * {@link SasFileConstants#LITTLE_ENDIAN_CHECKER} or {@link SasFileConstants#BIG_ENDIAN_CHECKER}
    * the file is considered invalid.
    *
    * @return true if the value of encoding equals to { @link SasFileConstants#LITTLE_ENDIAN_CHECKER}
    *         or { @link SasFileConstants#BIG_ENDIAN_CHECKER}
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
  def readSasFileHeader(fileStream: SasPageReader): ParseResult = {

    val offsetForAlign = Seq(Align1Offset, Align2Offset)
    val lengthForAlign = Seq(Align1Length, Align2Length)
    val resU64 = fileStream.readBytes(0, offsetForAlign, lengthForAlign, false)

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

    val res = fileStream.readBytes(resU64.relPosition.get, offset, length, false)

    val endianeness = res.readBytes(0)(0)

    if (!isSasFileValid(endianeness))
      throw new IOException(FileNotValid)

    val isEncodingPresent = SasCharacterEncodings.contains(res.readBytes(1)(0))
    val encoding = if (isEncodingPresent) SasCharacterEncodings(res.readBytes(1)(0)) else DefaultEncoding

    val properties = SasFileProperties(
      isU64, None, endianeness, encoding, null, bytesToString(res.readBytes(2), encoding).trim,
      bytesToString(res.readBytes(3), encoding).trim, bytesToDateTime(res.readBytes(4), endianeness),
      bytesToDateTime(res.readBytes(5), endianeness), bytesToString(res.readBytes(9), encoding).trim,
      bytesToString(res.readBytes(10), encoding).trim,
      if (res.readBytes(13)(0) != 0)
        bytesToString(res.readBytes(13), encoding).trim
      else
        bytesToString(res.readBytes(12), encoding).trim,
      bytesToString(res.readBytes(11), encoding).trim, bytesToInt(res.readBytes(6), endianeness),
      bytesToInt(res.readBytes(7), endianeness), bytesToLong(res.readBytes(8), isU64, endianeness)
    )

    if (fileStream != null)
      fileStream.moveToEndOfHeader(properties, res).get

    ParseResult(properties, properties.headerLength)
  }

  /**
    * The method to read page metadata and store it in {@link SasFileParser#currentPageType},
    * {@link SasFileParser#currentPageBlockCount} and {@link SasFileParser#currentPageSubheadersCount}.
    *
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} string is impossible.
    */
  @throws[IOException]
  def readPageHeader(fileStream: SasPageReader, properties: SasFileProperties): PageHeader = {
    val bitOffset = getBitOffset(properties)
    val offset = Seq(bitOffset + PAGE_TYPE_OFFSET, bitOffset + BLOCK_COUNT_OFFSET, bitOffset + SUBHEADER_COUNT_OFFSET)
    val length = Seq(PAGE_TYPE_LENGTH, BLOCK_COUNT_LENGTH, SUBHEADER_COUNT_LENGTH)
    val vars = fileStream.readBytes(0, offset, length)

    val pageType = bytesToShort(vars.readBytes(0), properties.endianness)
    val pageBlockCount = bytesToShort(vars.readBytes(1), properties.endianness)
    val pageSubheadersCount = bytesToShort(vars.readBytes(2), properties.endianness)

    logger.debug(PageType.format(pageType))
    logger.debug(BlockCount.format(pageBlockCount))
    logger.debug(SubheaderCount.format(pageSubheadersCount))

    PageHeader(pageType, pageBlockCount, pageSubheadersCount, vars.eof)
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
  private def processByteArrayWithData(page: Seq[Byte], properties: SasFileProperties, rowOffset: Long, rowLength: Long): Seq[Option[Any]] = {

    val source = {
      if (properties.isCompressed && rowLength < properties.rowLength)
        (LiteralsToDecompressor(properties.compressionMethod.get).
          decompressRow(rowOffset.toInt, rowLength.toInt, properties.rowLength.toInt, page), 0)
      else
        (page, rowOffset.toInt)
    }

    (0 until properties.columnsCount.toInt).
      takeWhile(properties.columnAttributes(_).length != 0).
      map(processElement(source._1, properties, source._2, _))

  }

  /**
    * The function to process element of row.
    *
    * @param source             an array of bytes containing required data.
    * @param offset             the offset in source of required data.
    * @param currentColumnIndex index of the current element.
    * @return object storing the data of the element.
    */
  private def processElement(source: Seq[Byte], properties: SasFileProperties, offset: Int, currentColumnIndex: Int, byteOutput: Boolean = false): Option[Any] = {
    val length = properties.columnAttributes(currentColumnIndex).length

    if (properties.getColumns()(currentColumnIndex)._type == Some(classOf[Number])) {
      val attOffset = properties.columnAttributes(currentColumnIndex).offset.toInt
      val temp = source.slice(offset + attOffset, offset + attOffset + length)

      if (length <= 2)
        Some(bytesToShort(temp, properties.endianness))
      else {
        properties.getColumns()(currentColumnIndex).format.name match {
          case Left(e) if DateTimeFormatStrings.contains(e) => Some(bytesToDateTime(temp, properties.endianness))
          case Left(e) if DateFormatStrings.contains(e) => Some(bytesToDate(temp, properties.endianness))
          case _ => Some(convertByteArrayToNumber(temp, properties.endianness))
        }
      }
    }
    else {
      val bytes = trimBytesArray(source, offset + properties.columnAttributes(currentColumnIndex).offset.toInt, length)

      if (byteOutput)
        Some(bytes)
      else {
        try
            if (bytes.isEmpty) None else Some(bytesToString(bytes, properties.encoding))
        catch {
          case e: UnsupportedEncodingException => {
            logger.error(e.getMessage, e)
            None
          }
        }
      }
    }
  }

  private def getBitOffset(properties: SasFileProperties): Int =
    if (properties.isU64) PageBitOffsetX64 else PageBitOffsetX86

  private def getSubheaderPointerLength(properties: SasFileProperties): Int =
    if (properties.isU64) SubheaderPointerLengthX64 else SubheaderPointerLengthX86

  /**
    * The method that reads and parses metadata from the sas7bdat and puts the results in
    * {@link SasFileParser#sasFileProperties}.
    *
    * @throws IOException - appears if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def getMetadataFromSasFile(fileStream: SasPageReader): SasMetadata = {

    val fileHeader = readSasFileHeader(fileStream)
    val pageHeader = readPageHeader(fileStream, fileHeader.properties)

    val metaResults = processSasFilePageMeta(fileStream, SasMetadata(pageHeader, fileHeader.properties))

    //todo: merge with process next page
    def iterateThroughPages(stream: SasPageReader, res: MetadataReadResult): SasFileProperties = {
      if (res.success)
        res.properties
      else {
        fileStream.cachePage(metaResults.properties).get
        val newPageHeader = readPageHeader(fileStream, metaResults.properties)
        val newRes = processSasFilePageMeta(fileStream, SasMetadata(newPageHeader, metaResults.properties))
        iterateThroughPages(stream, newRes)
      }
    }

    SasMetadata(pageHeader, iterateThroughPages(fileStream, metaResults))
  }

  /**
    * The method to read pages of the sas7bdat file. First, the method reads the page type
    * (at the {@link SasFileConstants#PageTypeOffset} offset), the number of rows on the page
    * (at the {@link SasFileConstants#BlockCountOffset} offset), and the number of subheaders
    * (at the {@link SasFileConstants#SubheaderCountOffset} offset). Then, depending on the page type,
    * the method calls the function to process the page.
    *
    * @return true if all metadata is read.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def processSasFilePageMeta(fileStream: SasPageReader, meta: SasMetadata): MetadataReadResult = {

    val res: Map[Option[SubheaderIndexes], Seq[SasFileProperties]] =
      if ((meta.pageHeader.pageType == PageMetaType1) || (meta.pageHeader.pageType == PageMetaType2) || (meta.pageHeader.pageType == PageMixType))
        readPageMetadata(fileStream, SasMetadata(meta.pageHeader, meta.properties))
      else
        Map()

    val propertiesNew =
      if (!res.isEmpty) {
        meta.properties.copy(
          rowLength = res.getOrElse(Some(SubheaderIndexes.RowSizeSubheaderIndex), Seq(meta.properties)).head.rowLength,
          rowCount = res.getOrElse(Some(SubheaderIndexes.RowSizeSubheaderIndex), Seq(meta.properties)).head.rowCount,
          mixPageRowCount = res.getOrElse(Some(SubheaderIndexes.RowSizeSubheaderIndex), Seq(meta.properties)).head.mixPageRowCount,
          columnsCount = res.getOrElse(Some(SubheaderIndexes.ColumnSizeSubheaderIndex), Seq(meta.properties)).head.columnsCount,
          compressionMethod = res.getOrElse(Some(SubheaderIndexes.ColumnTextSubheaderIndex), Seq(meta.properties)).head.compressionMethod,
          columnNamesBytes = res.getOrElse(Some(SubheaderIndexes.ColumnTextSubheaderIndex), Seq(meta.properties)).head.columnsNamesBytes,
          columnNames = res.getOrElse(Some(SubheaderIndexes.ColumnNameSubheaderIndex), Seq(meta.properties)).head.columnNames,
          columnAttributes = res.getOrElse(Some(SubheaderIndexes.ColumnAttributesSubheaderIndex), Seq(meta.properties)).head.columnAttributes,
          columnFormats = res.getOrElse(Some(SubheaderIndexes.FormatAndLabelSubheaderIndex), Seq(meta.properties)).flatMap(_.columnFormats),
          columnLabels = res.getOrElse(Some(SubheaderIndexes.FormatAndLabelSubheaderIndex), Seq(meta.properties)).flatMap(_.columnLabels),
          dataSubheaderPointers = res.getOrElse(Some(SubheaderIndexes.DataSubheaderIndex), Seq()).flatMap(_.dataSubheaderPointers)
        )
      }
      else
        meta.properties

    if (meta.pageHeader.pageType == PageDataType || meta.pageHeader.pageType == PageMixType || res.contains(Some(SubheaderIndexes.DataSubheaderIndex)))
      MetadataReadResult(true, propertiesNew.setColumns(propertiesNew.getColumns(false))) //Caching the columns here might be dangerous
    else if (meta.pageHeader.pageType == PageAmdType)
      MetadataReadResult(true, propertiesNew.setColumns(propertiesNew.getColumns(false, true)))
    else
      MetadataReadResult(false, propertiesNew)
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
  def readPageMetadata(fileStream: SasPageReader, meta: SasMetadata): Map[Option[SubheaderIndexes], Seq[SasFileProperties]] = {
    //todo: Make non copying work
    def getNewProperties(subheaderIndex: Option[SubheaderIndexes],
                         subheaderPointer: SubheaderPointer,
                         curProperties: SasFileProperties,
                         cond: Boolean): SasFileProperties = {

      if (subheaderIndex.isDefined) {
        logger.debug(SubheaderProcessFunctionName.format(subheaderIndex.get))
        if (cond)
          subheaderIndexToClass(subheaderIndex.get).processSubheader(fileStream, curProperties, subheaderPointer.offset, subheaderPointer.length, true)
        else
          curProperties
      }
      else {
        logger.debug(UnknownSubheaderSignature)
        curProperties
      }
    }

    def process(pointer: SubheaderPointer,
                curProperties: SasFileProperties,
                cond: Option[SubheaderIndexes] => Boolean): PageMetadataReadResult = {

      val subheaderSignature = readSubheaderSignature(fileStream, curProperties, pointer.offset)
      val subheaderIndex = chooseSubheaderClass(curProperties, subheaderSignature, pointer.compression, pointer._type)

      PageMetadataReadResult(subheaderIndex, pointer, getNewProperties(subheaderIndex, pointer, curProperties, cond(subheaderIndex)))
    }

    def processHeaders(curProperties: SasFileProperties, filter: Option[SubheaderIndexes] => Boolean,
                       runInParallel: Boolean = true): Stream[PageMetadataReadResult] = {
      val range = 0 until meta.pageHeader.subheaderCount
      val processFunc = {i: Int => {
        val subheaderPointer = readSubheaderPointer(fileStream, curProperties, i)

        if (subheaderPointer.compression != TruncatedSubheaderId)
          process(subheaderPointer, curProperties, filter)
        else
          PageMetadataReadResult(None, subheaderPointer, curProperties)
      }}

      if (fileStream.isParallelizable && runInParallel)
        range.par.map(processFunc).toStream
      else
        range.iterator.map(processFunc).toStream

    }

    //Handle the column text subheader first since it is a dependency to others; since it's one of the first headers it
    //makes no sense to search for it in parallel
    val colTextSubheader =
      processHeaders(meta.properties, _ == Some(SubheaderIndexes.ColumnTextSubheaderIndex), false).
        find(_.subheaderIndex == Some(SubheaderIndexes.ColumnTextSubheaderIndex))

    val adjProperties = {
      if (colTextSubheader.isDefined)
        meta.properties.
          setCompressionMethod(colTextSubheader.head.properties.compressionMethod).
          setColumnsNamesBytes(colTextSubheader.head.properties.columnsNamesBytes)
      else
        meta.properties
    }

    val results = processHeaders(
      adjProperties,
      _ != Some(SubheaderIndexes.DataSubheaderIndex))

    //todo: Sort the results since they can be read in parallel

    val col = results.groupBy(_.subheaderIndex)

    col.keys.map(key => key match {
      case Some(SubheaderIndexes.DataSubheaderIndex) =>
        key -> col(key).map(e => e.properties.setDataSubheaderPointers(Seq(e.pointer)))
      case _ =>
        key -> col(key).map(_.properties)
    }).toMap
  }

  /**
    * The function to read a subheader signature at the offset known from its ({@link SubheaderPointer}).
    *
    * @param subheaderPointerOffset the offset at which the subheader is located.
    * @return - the subheader signature to search for in the { @link SasFileParser#SubheaderSignatureToIndex} mapping later.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def readSubheaderSignature(fileStream: SasPageReader, properties: SasFileProperties, pointerOffset: Long): Long = {
    val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
    val offsetMass = Seq(pointerOffset)
    val lengthMass = Seq(intOrLongLength)

    val res = fileStream.readBytes(0, offsetMass, lengthMass)

    bytesToLong(res.readBytes(0), properties.isU64, properties.endianness)
  }

  /**
    * The function to determine the subheader type by its signature, {@link SubheaderPointer#compression},
    * and {@link SubheaderPointer#type}.
    *
    * @param signature the subheader signature to search for in the { @link SasFileParser#SubheaderSignatureToIndex} mapping
    * @param compression the type of subheader compression ({ @link SubheaderPointer#compression})
    * @param type        the subheader type ({ @link SubheaderPointer#_type})
    * @return an element from the  { @link SubheaderIndexes} enumeration that defines the type of the current subheader
    */
  def chooseSubheaderClass(properties: SasFileProperties, signature: Long, compression: Int, _type: Int): Option[SubheaderIndexes] = {
    if (!SubheaderSignatureToIndex.contains(signature)) {
      if (properties.isCompressed && (compression == CompressedSubheaderId || compression == 0) && _type == CompressedSubheaderType)
        Some(SubheaderIndexes.DataSubheaderIndex)
      else
        None
    }
    else
      Some(SubheaderSignatureToIndex(signature))
  }

  /**
    * The function to read the pointer with the subheaderPointerIndex index from the list of {@link SubheaderPointer}
    * located at the subheaderPointerOffset offset.
    *
    * @param pointerOffset the offset before the list of { @link SubheaderPointer}.
    * @param pointerIndex the index of the subheader pointer being read.
    * @return the subheader pointer.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def readSubheaderPointer(fileStream: SasPageReader, properties: SasFileProperties, pointerIndex: Int)
                          (implicit pointerOffset: Long = getBitOffset(properties).toLong + SubheaderPointersOffset): SubheaderPointer = {

    val intOrLongLength = if (properties.isU64) BytesInLong else BytesInInt
    val subheaderPointerLength = getSubheaderPointerLength(properties)
    val totalOffset = pointerOffset + subheaderPointerLength * pointerIndex.toLong

    val offset = Seq(totalOffset, totalOffset + intOrLongLength, totalOffset + 2L * intOrLongLength, totalOffset + 2L * intOrLongLength + 1)
    val length = Seq(intOrLongLength, intOrLongLength, 1, 1)

    val vars = fileStream.readBytes(0, offset, length).readBytes

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

  /**
    * Reads all rows from the sas7bdat file. For each row, only the columns defined in the list are read.
    *
    * @param columnNames list of column names which should be processed.
    * @return an array of array objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    */
  def readAll(fileStream: SasPageReader, meta: SasMetadata): Seq[Seq[Option[Any]]] = {

    def processPage(curMeta: SasMetadata): Seq[(Int, Try[PageRowReadResult])] = {
      val processFunc = {i: Int => {
        try {
          (i, Success(readNext(fileStream, curMeta, i)))
        }
        catch {
          case e: Throwable => (i, Failure(e))
        }
      }}

      if (fileStream.isParallelizable)
        (0 until curMeta.getPageRows).par.map(processFunc).toStream.sortBy(_._1)
      else
        (0 until curMeta.getPageRows).map(processFunc).toStream
    }

    def read(total: Long, curMeta: SasMetadata, acc: Seq[Seq[Option[Any]]]): Seq[Seq[Option[Any]]] = {

      if (total < meta.properties.rowCount) {

        val processRes = processPage(curMeta)
        val failedRead = processRes.find(_._2.isFailure)

        val res = {
          if (failedRead.isDefined) {
            failedRead match {
              case Some((index, e)) =>
                logger.warn("Exception encountered, skipping the rest of the file. " +
                  "Rows read on page: " + index + ". Expected number of rows from metadata: " + meta.properties.rowCount, e.failed.get)
                return acc ++ processRes.slice(0, index).map(_._2.get.row)
              case _ => processRes.map(_._2.get)
            }
          }
          else
            processRes.map(_._2.get)
        }

        logger.info("Total Records processed: %s".format(total))

        if (total < meta.properties.rowCount - 1)
          read(total + res.size, readNextPage(fileStream, curMeta.properties), acc ++ res.map(_.row))
        else
          acc ++ res.map(_.row)
      }
      else
        acc
    }

    read(0L, meta, Seq())
  }

  /**
    * The function to read next row from current sas7bdat file.
    *
    * @param columnNames list of column names which should be processed.
    * @return the object array containing elements of current row.
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def readNext(fileStream: SasPageReader, meta: SasMetadata, currentRowOnPageIndex: Int = 0): PageRowReadResult = {
    def isLastRowOnPage(index: Int = 0, meta: SasMetadata): Boolean = index == meta.getPageRows

    if (meta.pageHeader.eof) return PageRowReadResult(Seq(), true)
    val bitOffset = getBitOffset(meta.properties)

    meta.pageHeader.pageType match {
      case PageMetaType1 | PageMetaType2 => {
        require(!meta.properties.dataSubheaderPointers.isEmpty, "The data subheader pointers can't be missing")
        val dataSubheaderPointer = meta.properties.dataSubheaderPointers(currentRowOnPageIndex)
        //Assumes that the file stream is at the beginning of the page
        val row = subheaderIndexToClass(SubheaderIndexes.DataSubheaderIndex).
          processSubheader(fileStream, meta.properties, dataSubheaderPointer.offset, dataSubheaderPointer.length, false).row

        PageRowReadResult(row, isLastRowOnPage(currentRowOnPageIndex, meta))
      }
      case PageMixType => {
        val subheaderPointerLength = getSubheaderPointerLength(meta.properties)
        val alignCorrection = (bitOffset + SubheaderPointersOffset + meta.pageHeader.subheaderCount * subheaderPointerLength) % BitsInByte
        val offset = bitOffset + SubheaderPointersOffset + alignCorrection +
          meta.pageHeader.subheaderCount * subheaderPointerLength + currentRowOnPageIndex * meta.properties.rowLength

        val page = fileStream.readBytes(0, Seq(offset), Seq(meta.properties.pageLength)).readBytes
        val row = processByteArrayWithData(page(0), meta.properties, 0L, meta.properties.rowLength)

        PageRowReadResult(row, isLastRowOnPage(currentRowOnPageIndex, meta))
      }
      case PageDataType => {
        val offset = bitOffset + SubheaderPointersOffset + currentRowOnPageIndex * meta.properties.rowLength

        val page = fileStream.readBytes(0, Seq(offset), Seq(meta.properties.pageLength)).readBytes
        val row = processByteArrayWithData(page(0), meta.properties, 0L, meta.properties.rowLength)

        PageRowReadResult(row, isLastRowOnPage(currentRowOnPageIndex, meta))
      }
    }
  }

  /**
    * The method to read next page from sas7bdat file and put it into {@link SasFileParser#cachedPage}. If this page
    * has {@link SasFileConstants#PageMetaType1} or {@link SasFileConstants#PageMetaType2} type method process
    * it's subheaders. Method skips page with type other than {@link SasFileConstants#PageMetaType1},
    * {@link SasFileConstants#PageMetaType2}, {@link SasFileConstants#PageMixType} or
    * {@link SasFileConstants#PageDataType} and reads next.
    *
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} stream is impossible.
    */
  @throws[IOException]
  def readNextPage(inputStream: SasPageReader, properties: SasFileProperties): SasMetadata = {
    val meta = processNextPage(inputStream, properties)

    if (meta.pageHeader.pageType != PageMetaType1 && meta.pageHeader.pageType != PageMetaType2 &&
      meta.pageHeader.pageType != PageMixType && meta.pageHeader.pageType != PageDataType && !meta.pageHeader.eof)
      readNextPage(inputStream, properties)
    else
      meta
  }

  /**
    * Put next page to cache and read it's header.
    *
    * @throws IOException if reading from the { @link SasFileParser#sasFileStream} string is impossible.
    */
  @throws[IOException]
  private def processNextPage(inputStream: SasPageReader, properties: SasFileProperties): SasMetadata = {

    inputStream.cachePage(properties).get
    val page = readPageHeader(inputStream, properties)

    if ((page.pageType == PageMetaType1) || (page.pageType == PageMetaType2) || (page.pageType == PageAmdType))
      SasMetadata(page, processSasFilePageMeta(inputStream, SasMetadata(page, properties)).properties)
    else
      SasMetadata(page, properties)
  }
}