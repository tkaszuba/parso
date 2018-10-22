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

    val offsetForAlign = Seq(ALIGN_1_OFFSET, ALIGN_2_OFFSET)
    val lengthForAlign = Seq(ALIGN_1_LENGTH, ALIGN_2_LENGTH)

    val res = SasFileParser.getBytesFromFile(inputStream, 0, offsetForAlign, lengthForAlign)
    val eof = res.eof
    val vars = res.readBytes

    assert(!eof)
    assert(vars(0)(0).toHexString == "22")
    assert(vars(1)(0).toHexString == "32")

    inputStream.close()
  }

  it should "return the proper bytes given a previously parsed page" in {
    ???
    //val is = getResourceAsStream("sas7bdat/mixed_data_one.sas7bdat")
  }

  "Checking if a sas file is valid" should "return the proper validity" in {
    assert(SasFileParser.isSasFileValid(LITTLE_ENDIAN_CHECKER))
    assert(SasFileParser.isSasFileValid(BIG_ENDIAN_CHECKER))
    assert(!SasFileParser.isSasFileValid(-1))
  }

  "Converting a byte array" should "return proper string values" in {
    //val bytes = Seq('±','²','³','´','µ').map(_.toByte)
    val bytes = Seq(177, 178, 179, 180, 181).map(_.toByte)

    assert(SasFileParser.bytesToString(bytes, "CP1252") == "±²³´µ")
  }

  it should "return proper byte values based on the endianes" in {
    val bytes = Seq(0x45,0x67,0x98,0x12).map(_.toByte)

    val little = SasFileParser.byteArrayToByteBuffer(bytes, LITTLE_ENDIAN_CHECKER)
    assert(little.array().toSeq == bytes)
    assert(little.order() == ByteOrder.LITTLE_ENDIAN)
    val big = SasFileParser.byteArrayToByteBuffer(bytes, BIG_ENDIAN_CHECKER)
    assert(big.array().toSeq == bytes)
    assert(big.order() == ByteOrder.BIG_ENDIAN)
  }

  it should "return proper integer values" in {
    val bytes = Seq(0x45,0x67,0x98,0x12).map(_.toByte)

    assert(SasFileParser.bytesToInt(bytes, LITTLE_ENDIAN_CHECKER) == 311977797)
    assert(SasFileParser.bytesToInt(bytes, BIG_ENDIAN_CHECKER) == 1164417042)
  }

  it should "return proper short values" in {
    val bytes = Seq(0x20,0x12).map(_.toByte)

    assert(SasFileParser.bytesToShort(bytes, LITTLE_ENDIAN_CHECKER) == 4640)
    assert(SasFileParser.bytesToShort(bytes, BIG_ENDIAN_CHECKER) == 8210)
  }

  it should "return proper long values" in {
    val bytes = Seq(07F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF).map(_.toByte)

    assert(SasFileParser.bytesToLong(bytes, true, BIG_ENDIAN_CHECKER) == 576460752303423487L)
    assert(SasFileParser.bytesToLong(bytes, false, BIG_ENDIAN_CHECKER) == 134217727L)
  }

  it should "return proper double values" in {
    val bytes = Seq(0x20,0x12).map(_.toByte)

    assert(SasFileParser.bytesToDouble(bytes, BIG_ENDIAN_CHECKER) == 3.356253329040093E-154)
    assert(SasFileParser.bytesToDouble(bytes, LITTLE_ENDIAN_CHECKER) == 2.2131618651272261E-221)
  }

  it should "return proper date time" in {
    val bytes = Seq[Byte](0,0,0,64,78,111,-47,65) //1997-01-28T00:00Z
    val date = ZonedDateTime.of(
      1997,1,28,0,0,0,0, ZoneOffset.UTC)

    assert(SasFileParser.bytesToDateTime(bytes, LITTLE_ENDIAN_CHECKER) == date)
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

    val res = SasFileParser.processSasFileHeader(inputStream)

    assert(res == properties)

    inputStream.close()
  }


}