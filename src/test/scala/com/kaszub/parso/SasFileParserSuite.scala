package com.kaszub.parso

import java.io.{BufferedInputStream, InputStream}
import java.nio.ByteOrder
import java.time.{ZoneOffset, ZonedDateTime}

import com.kaszub.parso.impl.SasFileParser.SubheaderIndexes.SubheaderIndexes
import com.kaszub.parso.impl.SasFileParser.{SasMetadata, SubheaderIndexes, SubheaderPointer, subheaderIndexToClass}
import com.kaszub.parso.impl.{SasFileConstants, SasFileParser}
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

import scala.collection.immutable.ArraySeq

class SasFileParserSuite extends FlatSpec with SasFileConstants {

  private val logger = Logger[this.type]

  private val DefaultFileName = "sas7bdat//all_rand_normal.sas7bdat"
  private val NoCompressionFileName = "sas7bdat//charset_aara.sas7bdat"
  private val ColonFileName = "sas7bdat//colon.sas7bdat"

  private val DefaultFileColumns = Vector(
    Column(Some(0), Some("x1"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(1), Some("x2"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(2), Some("x3"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(3), Some("x4"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(4), Some("x5"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(5), Some("x6"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(6), Some("x7"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)),
    Column(Some(7), Some("x8"), ColumnLabel(Left("")), ColumnFormat(Left(""),0,0), Some(classOf[java.lang.Number]), Some(8)))

  private val DefaultDataPointers = Vector(
    SubheaderPointer(2478,58,4,1), SubheaderPointer(2418,60,4,1), SubheaderPointer(2359,59,4,1), SubheaderPointer(2299,60,4,1),
    SubheaderPointer(2239,60,4,1), SubheaderPointer(2179,60,4,1), SubheaderPointer(2119,60,4,1), SubheaderPointer(2059,60,4,1),
    SubheaderPointer(1999,60,4,1), SubheaderPointer(1939,60,4,1), SubheaderPointer(1879,60,4,1), SubheaderPointer(1819,60,4,1),
    SubheaderPointer(1759,60,4,1), SubheaderPointer(1699,60,4,1), SubheaderPointer(1639,60,4,1), SubheaderPointer(1579,60,4,1),
    SubheaderPointer(1518,61,4,1), SubheaderPointer(1458,60,4,1), SubheaderPointer(1397,61,4,1), SubheaderPointer(1337,60,4,1),
    SubheaderPointer(1277,60,4,1), SubheaderPointer(1216,61,4,1), SubheaderPointer(1156,60,4,1), SubheaderPointer(1095,61,4,1),
    SubheaderPointer(1034,61,4,1), SubheaderPointer(973,61,4,1), SubheaderPointer(913,60,4,1), SubheaderPointer(852,61,4,1),
    SubheaderPointer(791,61,4,1), SubheaderPointer(730,61,4,1), SubheaderPointer(669,61,4,1), SubheaderPointer(608,61,4,1))

  private val DefaultFileMetadata = SasFileProperties(
    false, Some("SASYZCRL"), 1, "windows-1251", null,
    "ALL_RAND_NORMAL", "DATA",
    ZonedDateTime.of(
      1993, 6, 5, 21, 36, 17, 0, ZoneOffset.UTC),
    ZonedDateTime.of(
      1993, 6, 5, 21, 36, 17, 0, ZoneOffset.UTC),
    "9.0101M3", "XP_PRO", "", "",
    1024, 4096, 3, 64,
    37, 36, 8, Seq(ArraySeq(76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 83, 65, 83, 89, 90, 67, 82, 76, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 68, 65, 84, 65, 83, 84, 69, 80, 120, 49, 0, 0, 120, 50, 0, 0, 120, 51, 0, 0, 120, 52, 0, 0, 120, 53, 0, 0, 120, 54, 0, 0, 120, 55, 0, 0, 120, 56, 0, 0)),
    Vector(Left("x1"), Left("x2"), Left("x3"), Left("x4"), Left("x5"), Left("x6"), Left("x7"), Left("x8")),
    Vector(ColumnAttributes(0L, 8, classOf[java.lang.Number]), ColumnAttributes(8L, 8, classOf[java.lang.Number]), ColumnAttributes(16L, 8, classOf[java.lang.Number]), ColumnAttributes(24L, 8, classOf[java.lang.Number]), ColumnAttributes(32L, 8, classOf[java.lang.Number]), ColumnAttributes(40L, 8, classOf[java.lang.Number]), ColumnAttributes(48L, 8, classOf[java.lang.Number]), ColumnAttributes(56L, 8, classOf[java.lang.Number])),
    Vector(ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0),ColumnFormat(Left(""),0,0)),
    Vector(ColumnLabel(Left("")),ColumnLabel(Left("")),ColumnLabel(Left("")),ColumnLabel(Left("")),ColumnLabel(Left("")),ColumnLabel(Left("")),ColumnLabel(Left("")),ColumnLabel(Left(""))),
    DefaultFileColumns, DefaultDataPointers, Seq()
  )

  private def DefaultFileNameStream: InputStream = new BufferedInputStream(
    Thread.currentThread.getContextClassLoader.getResourceAsStream(DefaultFileName))

  private def NoCompressionFileNameStream: InputStream = new BufferedInputStream(
    Thread.currentThread.getContextClassLoader.getResourceAsStream(NoCompressionFileName))

  private def ColonFileNameStream: InputStream = new BufferedInputStream(
    Thread.currentThread.getContextClassLoader.getResourceAsStream(ColonFileName))

  "Reading bytes from a SAS file" should "return the proper bytes" in {
    val inputStream = DefaultFileNameStream

    val offsetForAlign = Seq(Align1Offset, Align2Offset)
    val lengthForAlign = Seq(Align1Length, Align2Length)

    val res = SasFileParser.getBytesFromFile(inputStream, 0, offsetForAlign, lengthForAlign, false)
    val eof = res.eof
    val vars = res.readBytes

    assert(!eof)
    assert(res.relPosition == offsetForAlign(1) + lengthForAlign(1))
    assert(res.absPosition == offsetForAlign(0) + lengthForAlign(0) + offsetForAlign(1) + lengthForAlign(1))
    assert(vars(0)(0).toHexString == "22")
    assert(vars(1)(0).toHexString == "32")

    inputStream.close()
  }

  it should "return the proper bytes given a previously parsed page" in {
    ???
    //val is = getResourceAsStream("sas7bdat/mixed_data_one.sas7bdat")
  }

  "Checking if a sas file is valid" should "return the proper validity" in {
    assert(SasFileParser.isSasFileValid(LittleEndianChecker))
    assert(SasFileParser.isSasFileValid(BigEndianChecker))
    assert(!SasFileParser.isSasFileValid(-1))
  }

  "Converting a byte array" should "return proper string values" in {
    //val bytes = Seq('±','²','³','´','µ').map(_.toByte)
    val bytes = Seq(177, 178, 179, 180, 181).map(_.toByte)

    assert(SasFileParser.bytesToString(bytes, "CP1252") == "±²³´µ")
  }

  it should "return proper byte values based on the endianes" in {
    val bytes = Seq(0x45, 0x67, 0x98, 0x12).map(_.toByte)

    val little = SasFileParser.byteArrayToByteBuffer(bytes, LittleEndianChecker)
    assert(little.array().toSeq == bytes)
    assert(little.order() == ByteOrder.LITTLE_ENDIAN)
    val big = SasFileParser.byteArrayToByteBuffer(bytes, BigEndianChecker)
    assert(big.array().toSeq == bytes)
    assert(big.order() == ByteOrder.BIG_ENDIAN)
  }

  it should "return proper integer values" in {
    val bytes = Seq(0x45, 0x67, 0x98, 0x12).map(_.toByte)

    assert(SasFileParser.bytesToInt(bytes, LittleEndianChecker) == 311977797)
    assert(SasFileParser.bytesToInt(bytes, BigEndianChecker) == 1164417042)
  }

  it should "return proper short values" in {
    val bytes = Seq(0x20, 0x12).map(_.toByte)

    assert(SasFileParser.bytesToShort(bytes, LittleEndianChecker) == 4640)
    assert(SasFileParser.bytesToShort(bytes, BigEndianChecker) == 8210)
  }

  it should "return proper long values" in {
    val bytes = Seq(07F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF).map(_.toByte)

    assert(SasFileParser.bytesToLong(bytes, true, BigEndianChecker) == 576460752303423487L)
    assert(SasFileParser.bytesToLong(bytes, false, BigEndianChecker) == 134217727L)
  }

  it should "return proper double values" in {
    val bytes = Seq(0x20, 0x12).map(_.toByte)

    assert(SasFileParser.bytesToDouble(bytes, BigEndianChecker) == 3.356253329040093E-154)
    assert(SasFileParser.bytesToDouble(bytes, LittleEndianChecker) == 2.2131618651272261E-221)
  }

  it should "return proper date time" in {
    val bytes = Seq[Byte](0, 0, 0, 64, 78, 111, -47, 65) //1997-01-28T00:00Z
    val date = ZonedDateTime.of(
      1997, 1, 28, 0, 0, 0, 0, ZoneOffset.UTC)

    assert(SasFileParser.bytesToDateTime(bytes, LittleEndianChecker) == date)
    //assert(SasFileParser.bytesToDate(bytes, LITTLE_ENDIAN_CHECKER) == date)

  }

  "Reading the header from a SAS file" should "return the proper header info" in {

    val ColonSasFileProperties = SasFileProperties(
      false, None, 1, "US-ASCII", null,
      "colon", "DATA",
      ZonedDateTime.of(
        1997, 1, 28, 0, 0, 0, 0, ZoneOffset.UTC),
      ZonedDateTime.of(
        1997, 1, 28, 0, 0, 0, 0, ZoneOffset.UTC),
      "7.00.00B", "WIN_95", "WIN", "",
      1024, 262144, 7, 15564,
      104, 2493, 13)

    val properties = ColonSasFileProperties.setRowLength(0).setRowCount(0).setMixPageRowCount(0).setColumnsCount(0)

    val inputStream = ColonFileNameStream

    val res = SasFileParser.readSasFileHeader(inputStream)

    assert(res.properties == properties)

    inputStream.close()
  }

  it should "move the position of the input stream to the end of the header" in {
    val file1 = ColonFileNameStream
    val file2 = ColonFileNameStream

    val res = SasFileParser.readSasFileHeader(file1)

    assert(res.properties.headerLength == 1024)

    val expected =
      SasFileParser.getBytesFromFile(file2, 0, Seq(res.properties.headerLength.toLong), Seq(30), false).readBytes

    val result =
      SasFileParser.getBytesFromFile(file1, 0, Seq(0L), Seq(30), false).readBytes

    assert(result == expected)

    file1.close()
    file2.close()
  }

  "Reading the Page metadata from a SAS file" should "return the proper header information" in {
    val file = ColonFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pageHeader = SasFileParser.readPageHeader(file, fileHeader.properties)

    assert(pageHeader.pageType == 512)
    assert(pageHeader.blockCount == 2514)
    assert(pageHeader.subheaderCount == 21)
  }

  it should "return the proper number of parsed pointers" in {
    val file = ColonFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pageHeader = SasFileParser.readPageHeader(file, fileHeader.properties)

    val pointers = (0 until pageHeader.subheaderCount).map(
      SasFileParser.readSubheaderPointer(file, fileHeader.properties, _))

    assert(pointers.length == pageHeader.subheaderCount)
    assert(pointers(0).offset == 261664)
    assert(pointers(0).length == 480)
    assert(pointers(0).compression == 0)
    assert(pointers(0)._type == 0)

    file.close()
  }

  it should "return the proper signature" in {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)

    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, 0)
    val signature = SasFileParser.readSubheaderSignature(file, fileHeader.properties, pointer.offset)

    assert(signature == -134744073)

    file.close()
  }

  it should "return the proper subheader index" in {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)

    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, 0)
    val signature = SasFileParser.readSubheaderSignature(file, fileHeader.properties, pointer.offset)
    val index = SasFileParser.chooseSubheaderClass(fileHeader.properties, signature, pointer.compression, pointer._type)

    assert(index == Some(SubheaderIndexes.RowSizeSubheaderIndex))

    val pointer2 = SasFileParser.readSubheaderPointer(file, fileHeader.properties, 1)
    val signature2 = SasFileParser.readSubheaderSignature(file, fileHeader.properties, pointer2.offset)
    val index2 = SasFileParser.chooseSubheaderClass(fileHeader.properties, signature2, pointer2.compression, pointer2._type)

    assert(index2 == Some(SubheaderIndexes.ColumnSizeSubheaderIndex))

    file.close()
  }

  private def getSubheaderProperties(index: SubheaderIndexes): SasFileProperties = {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, index.id)

    subheaderIndexToClass(index)
      .processSubheader(file, fileHeader.properties, pointer.offset, pointer.length)
  }

  it should "return the proper properties for a row size subheader" in {
    val properties = getSubheaderProperties(SubheaderIndexes.RowSizeSubheaderIndex)

    assert(properties.rowLength == 64)
    assert(properties.rowCount == 37)
    assert(properties.mixPageRowCount == 36)
  }

  it should "return the proper properties for a column size subheader" in {
    val properties = getSubheaderProperties(SubheaderIndexes.ColumnSizeSubheaderIndex)

    assert(properties.columnsCount == 8L)
  }

  it should "return the same properties for a counts subheader" in {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.SubheaderCountsSubheaderIndex.id)

    val properties = subheaderIndexToClass(SubheaderIndexes.SubheaderCountsSubheaderIndex)
      .processSubheader(file, fileHeader.properties, pointer.offset, pointer.length)

    assert(properties == fileHeader.properties)

    file.close()
  }

  it should "return the proper properties for a column text subheader with char compression" in {
    val properties = getSubheaderProperties(SubheaderIndexes.ColumnTextSubheaderIndex)

    assert(properties.compressionMethod == Option("SASYZCRL"))
    assert(properties.columnsNamesBytes.size == 1)
    assert(properties.columnsNamesBytes(0).size == 76)
  }

  it should "return the proper properties for a column text subheader with no compression" in {
    val file = NoCompressionFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.SubheaderCountsSubheaderIndex.id)

    val properties = subheaderIndexToClass(SubheaderIndexes.SubheaderCountsSubheaderIndex)
      .processSubheader(file, fileHeader.properties, pointer.offset, pointer.length)

    assert(properties.compressionMethod == None)
    assert(properties.columnsNamesBytes.size == 1)
    assert(properties.columnsNamesBytes(0).size == 0)
  }


  it should "return the proper column names" in {
    val file = DefaultFileNameStream
    val columns = Seq(Left("x1"), Left("x2"), Left("x3"), Left("x4"), Left("x5"), Left("x6"), Left("x7"), Left("x8"))

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pointerText = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.ColumnTextSubheaderIndex.id)
    val propertiesText = subheaderIndexToClass(SubheaderIndexes.ColumnTextSubheaderIndex)
      .processSubheader(file, fileHeader.properties, pointerText.offset, pointerText.length)

    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.ColumnNameSubheaderIndex.id)
    val properties = subheaderIndexToClass(SubheaderIndexes.ColumnNameSubheaderIndex)
      .processSubheader(file, propertiesText, pointer.offset, pointer.length)

    assert(properties.columnNames == columns)

    file.close()
  }

  it should "return the proper column attributes" in {
    val properties = getSubheaderProperties(SubheaderIndexes.ColumnAttributesSubheaderIndex)

    assert(properties.columnAttributes.size == 8)
  }

  it should "return the proper column and format properties" in {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pointerText = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.ColumnTextSubheaderIndex.id)
    val propertiesText = subheaderIndexToClass(SubheaderIndexes.ColumnTextSubheaderIndex)
      .processSubheader(file, fileHeader.properties, pointerText.offset, pointerText.length)

    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.FormatAndLabelSubheaderIndex.id)
    val properties = subheaderIndexToClass(SubheaderIndexes.FormatAndLabelSubheaderIndex)
      .processSubheader(file, propertiesText, pointer.offset, pointer.length)

    assert(properties.columnFormats.head.toString == ".40.")
    assert(properties.columnFormats.head.name == Right(ColumnMissingInfo(0,4,8,1, MissingInfoType.FORMAT)))
    assert(properties.columnFormats.head.precision == 0)
    assert(properties.columnFormats.head.width == 40)
    assert(properties.columnLabels.head.alias == Right(ColumnMissingInfo(0,5,0,2, MissingInfoType.LABEL)))

    file.close()
  }

  it should "return the same properties when reading the list subheader" in {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pointerText = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.ColumnTextSubheaderIndex.id)
    val propertiesText = subheaderIndexToClass(SubheaderIndexes.ColumnTextSubheaderIndex)
      .processSubheader(file, fileHeader.properties, pointerText.offset, pointerText.length)

    val pointer = SasFileParser.readSubheaderPointer(file, fileHeader.properties, SubheaderIndexes.ColumnListSubheaderIndex.id)
    val properties = subheaderIndexToClass(SubheaderIndexes.ColumnListSubheaderIndex)
      .processSubheader(file, propertiesText, pointer.offset, pointer.length)

    assert(properties == propertiesText)
  }

  it should "return the merged properties" in {
    val file = DefaultFileNameStream

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pageHeader = SasFileParser.readPageHeader(file, fileHeader.properties)

    val res = SasFileParser.processSasFilePageMeta(file, SasMetadata(pageHeader, fileHeader.properties))
    val cols = res.properties.getColumns()

    assert(res.success)
    assert(cols == DefaultFileColumns)
    assert(cols == res.properties.columns, "columns should be cached")
    assert(res.properties == DefaultFileMetadata)
  }

  it should "process all of metadata" in {
    val file = DefaultFileNameStream

    val properties = SasFileParser.getMetadataFromSasFile(file).properties
    assert(properties == DefaultFileMetadata)
  }

  it should "return the proper row when reading the data row subheader" in {
    val file = NoCompressionFileNameStream//DefaultFileNameStream

    val meta = SasFileParser.getMetadataFromSasFile(file)
    val row = SasFileParser.readNext(file, meta)

    assert(row == Vector(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa")))
  }

}