package com.kaszub.parso

import java.io.{BufferedInputStream, InputStream}
import java.nio.ByteOrder
import java.time.{ZoneOffset, ZonedDateTime}

import com.kaszub.parso.impl.SasFileParser.SubheaderIndexes.SubheaderIndexes
import com.kaszub.parso.impl.SasFileParser.{SubheaderIndexes, subheaderIndexToClass}
import com.kaszub.parso.impl.{SasFileConstants, SasFileParser}
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

class SasFileParserSuite extends FlatSpec with SasFileConstants {

  private val logger = Logger[this.type]

  private val DefaultFileName = "sas7bdat//all_rand_normal.sas7bdat"
  private val ColonFileName = "sas7bdat//colon.sas7bdat"

  private def DefaultFileNameStream: InputStream = new BufferedInputStream(
    Thread.currentThread().getContextClassLoader().getResourceAsStream(DefaultFileName))

  private def ColonFileNameStream: InputStream = new BufferedInputStream(
    Thread.currentThread().getContextClassLoader().getResourceAsStream(ColonFileName))

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

    val ColonSasFileProperties = new SasFileProperties(
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

    val pointers = (0 to pageHeader.subheaderCount - 1).map(
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

    assert(index == SubheaderIndexes.RowSizeSubheaderIndex)

    val pointer2 = SasFileParser.readSubheaderPointer(file, fileHeader.properties, 1)
    val signature2 = SasFileParser.readSubheaderSignature(file, fileHeader.properties, pointer2.offset)
    val index2 = SasFileParser.chooseSubheaderClass(fileHeader.properties, signature2, pointer2.compression, pointer2._type)

    assert(index2 == SubheaderIndexes.ColumnSizeSubheaderIndex)

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

  it should "return the proper properties for a column text subheader" in {
    val properties = getSubheaderProperties(SubheaderIndexes.ColumnTextSubheaderIndex)

    assert(properties.compressionMethod == Option("SASYZCRL"))
    assert(properties.columnsNamesBytes.size == 1)
    assert(properties.columnsNamesBytes(0).size == 76)
  }

  it should "return the proper column names" in {
    val file = DefaultFileNameStream
    var columns = Seq(Left("x1"), Left("x2"), Left("x3"), Left("x4"), Left("x5"), Left("x6"), Left("x7"), Left("x8"))

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




  }