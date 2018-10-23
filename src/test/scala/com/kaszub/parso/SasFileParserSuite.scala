package com.kaszub.parso

import java.io.BufferedInputStream
import java.nio.ByteOrder
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.kaszub.parso.impl.{SasFileConstants, SasFileParser}
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

import scala.io.Source

class SasFileParserSuite extends FlatSpec with SasFileConstants {

  private val logger = Logger[this.type]

  private val DefaultFileName = "sas7bdat//all_rand_normal.sas7bdat"
  private val ColonFileName = "sas7bdat//colon.sas7bdat"

  "Reading bytes from a SAS file" should "return the proper bytes" in {
    val inputStream = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader().getResourceAsStream(DefaultFileName)
    )

    val offsetForAlign = Seq(Align1Offset, Align2Offset)
    val lengthForAlign = Seq(Align1Length, Align2Length)

    val res = SasFileParser.getBytesFromFile(inputStream, 0, offsetForAlign, lengthForAlign)
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
    val bytes = Seq(0x45,0x67,0x98,0x12).map(_.toByte)

    val little = SasFileParser.byteArrayToByteBuffer(bytes, LittleEndianChecker)
    assert(little.array().toSeq == bytes)
    assert(little.order() == ByteOrder.LITTLE_ENDIAN)
    val big = SasFileParser.byteArrayToByteBuffer(bytes, BigEndianChecker)
    assert(big.array().toSeq == bytes)
    assert(big.order() == ByteOrder.BIG_ENDIAN)
  }

  it should "return proper integer values" in {
    val bytes = Seq(0x45,0x67,0x98,0x12).map(_.toByte)

    assert(SasFileParser.bytesToInt(bytes, LittleEndianChecker) == 311977797)
    assert(SasFileParser.bytesToInt(bytes, BigEndianChecker) == 1164417042)
  }

  it should "return proper short values" in {
    val bytes = Seq(0x20,0x12).map(_.toByte)

    assert(SasFileParser.bytesToShort(bytes, LittleEndianChecker) == 4640)
    assert(SasFileParser.bytesToShort(bytes, BigEndianChecker) == 8210)
  }

  it should "return proper long values" in {
    val bytes = Seq(07F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF).map(_.toByte)

    assert(SasFileParser.bytesToLong(bytes, true, BigEndianChecker) == 576460752303423487L)
    assert(SasFileParser.bytesToLong(bytes, false, BigEndianChecker) == 134217727L)
  }

  it should "return proper double values" in {
    val bytes = Seq(0x20,0x12).map(_.toByte)

    assert(SasFileParser.bytesToDouble(bytes, BigEndianChecker) == 3.356253329040093E-154)
    assert(SasFileParser.bytesToDouble(bytes, LittleEndianChecker) == 2.2131618651272261E-221)
  }

  it should "return proper date time" in {
    val bytes = Seq[Byte](0,0,0,64,78,111,-47,65) //1997-01-28T00:00Z
    val date = ZonedDateTime.of(
      1997,1,28,0,0,0,0, ZoneOffset.UTC)

    assert(SasFileParser.bytesToDateTime(bytes, LittleEndianChecker) == date)
    //assert(SasFileParser.bytesToDate(bytes, LITTLE_ENDIAN_CHECKER) == date)

  }

  "Reading the header from a SAS file" should "return the proper header info" in {

    val ColonSasFileProperties = new SasFileProperties(
      false, null, 1, "US-ASCII", null,
      "colon","DATA",
      ZonedDateTime.of(
        1997,1,28,0,0,0,0, ZoneOffset.UTC),
      ZonedDateTime.of(
        1997,1,28,0,0,0,0, ZoneOffset.UTC),
      "7.00.00B", "WIN_95", "WIN", "",
      1024, 262144, 7, 15564,
      104, 2493, 13)

    val properties = ColonSasFileProperties.setRowLength(0).setRowCount(0).setMixPageRowCount(0).setColumnsCount(0)

    val inputStream = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader().getResourceAsStream(ColonFileName)
    )

    val res = SasFileParser.readSasFileHeader(inputStream)

    assert(res.properties == properties)

    inputStream.close()
  }

  it should "move the position of the input stream to the end of the header" in {
    val file1 = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader().getResourceAsStream(ColonFileName))

    val file2 = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader().getResourceAsStream(ColonFileName))

    val res = SasFileParser.readSasFileHeader(file1)

    assert(res.properties.headerLength == 1024)

    val expected =
      SasFileParser.getBytesFromFile(file2, 0, Seq(res.properties.headerLength.toLong), Seq(30)).readBytes

    val result =
      SasFileParser.getBytesFromFile(file1, 0, Seq(0L), Seq(30)).readBytes

    assert(result == expected)

    file1.close()
    file2.close()
  }

  "Reading the Page header from a SAS file" should "return the proper results" in {
    val file = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader().getResourceAsStream(ColonFileName))

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pageHeader = SasFileParser.readPageHeader(file, fileHeader.properties)

    assert(pageHeader.pageType == 512)
    assert(pageHeader.blockCount == 2514)
    assert(pageHeader.subheaderCount == 21)
  }

  "Reading the subheader pointers" should "return the proper number of parsed pointers" in {
    val file = new BufferedInputStream(
      Thread.currentThread().getContextClassLoader().getResourceAsStream(ColonFileName))

    val fileHeader = SasFileParser.readSasFileHeader(file)
    val pageHeader = SasFileParser.readPageHeader(file, fileHeader.properties)

    val pointers = (0 to pageHeader.subheaderCount - 1).map(
      SasFileParser.readSubheaderPointer(file, fileHeader.properties, _))

    assert(pointers.length == pageHeader.subheaderCount)
    assert(pointers(0).offset == -52166656)
    assert(pointers(0).length == 19922947)
    assert(pointers(0).compression == 0)
    assert(pointers(0)._type == 0)

    file.close()
  }
}