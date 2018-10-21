package com.kaszub.parso

import java.io.BufferedInputStream

import com.kaszub.parso.impl.{SasFileConstants, SasFileParser}
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

import scala.io.Source

class SasFileParserSuite extends FlatSpec with SasFileConstants {

  private val logger = Logger[this.type]

  private val DefaultFileName = "sas7bdat//all_rand_normal.sas7bdat"

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
}