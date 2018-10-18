package com.kaszub.parso

import org.scalatest.FlatSpec

class DataWriterUtilSuite extends FlatSpec {

  val columnFormat = ColumnFormat("test", 1, 2)

  "A percent element " should "convert properly with no column format precision" in {
    val columnFormat = ColumnFormat("test", 1, 0)

    assert(DataWriterUtil.convertPercentElementToString(2.0, columnFormat) === "200%")
    assert(DataWriterUtil.convertPercentElementToString(2, columnFormat) === "200%")
    assert(DataWriterUtil.convertPercentElementToString(2L, columnFormat) === "200%")
    assert(DataWriterUtil.convertPercentElementToString(1.5, columnFormat) === "150%")
    assert(DataWriterUtil.convertPercentElementToString(0.997, columnFormat) === "100%")
    assert(DataWriterUtil.convertPercentElementToString(0.987, columnFormat) === "99%")
    assert(DataWriterUtil.convertPercentElementToString(0.496, columnFormat) === "50%")

  }

  it should "convert properly with column format precision" in {
    val columnFormat = ColumnFormat("test", 1, 2)

    assert(DataWriterUtil.convertPercentElementToString(2.0, columnFormat) === "200.00%")
    assert(DataWriterUtil.convertPercentElementToString(2, columnFormat) === "200.00%")
    assert(DataWriterUtil.convertPercentElementToString(2L, columnFormat) === "200.00%")
    assert(DataWriterUtil.convertPercentElementToString(1.5, columnFormat) === "150.00%")
    assert(DataWriterUtil.convertPercentElementToString(0.997, columnFormat) === "99.70%")
    assert(DataWriterUtil.convertPercentElementToString(0.987, columnFormat) === "98.70%")
    assert(DataWriterUtil.convertPercentElementToString(0.496, columnFormat) === "49.60%")
  }

  it should "produce a NumberFormatException if passed something other then a long, int or double" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString("a", ColumnFormat("test", 2, 0))
    }
  }

  "A numeral string" should "have trailing zeros trimmed" in {
    val tests = Set[(String, String)](
      ("0.10100", "0.101"), ("0.10", "0.1"), ("1.0", "1"), ("0.0", "0"), ("1.00000", "1"), ("1.01", "1.01"))

    tests.foreach(test => assert(DataWriterUtil.trimZerosFromEnd(test._1) === test._2))
  }

}
