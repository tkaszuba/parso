package com.kaszub.parso

import java.time.LocalDateTime

import org.scalatest.FlatSpec

class DataWriterUtilSuite extends FlatSpec {

  val columnFormat = ColumnFormat("test", 1, 2)

  "A percent element when converting to a string" should "convert properly with no column format precision" in {
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

  it should "produce a NumberFormatException when passed something other then a long, int or double" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString({}, ColumnFormat("test", 2, 0))
    }
  }

  it should "produce a NumberFormatException when passed a null" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString(null, ColumnFormat("test", 2, 0))
    }
  }

  it should "produce a NumberFormatException when passed a non convertible string" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString("a", ColumnFormat("test", 2, 0))
    }
  }

  "When trimming a numeral string it" should "have trailing zeros removed" in {
    val tests = Set[(String, String)](
      ("0.10100", "0.101"), ("0.10", "0.1"), ("1.0", "1"), ("0.0", "0"), ("1.00000", "1"), ("1.01", "1.01"))

    tests.foreach(test => assert(DataWriterUtil.trimZerosFromEnd(test._1) === test._2))
  }

  "A double element" should "convert properly to a string" in {

    val tests = Set[(Double, String)](
      (2.000456900891239, "2.00045690089124"), //Round
      (20004569008912.39, "2.00045690089124E13"), //Round
      (2456909d, "2456909"), //No Round
      (2456909.098, "2456909.098"), //No Round
      (20004569008912395d, "2.00045690089124E16"), //Round
    )

    tests.foreach(test =>
      assert(DataWriterUtil.convertDoubleElementToString(test._1) === test._2))
  }

  "A time element" should "convert properly to a string" in {
    val tests = Set[(Long, String)](
      (0, "00:00:00"),
      (10, "00:00:10"),
      (60, "00:01:00"),
      (70, "00:01:10"),
      (3600, "01:00:00"),
      (3670, "01:01:10")
    )

    tests.foreach(test =>
      assert(DataWriterUtil.convertTimeElementToString(test._1) === test._2))
  }

    it should "produce an AssertionException when the value is less than 0" in {
    intercept[AssertionError] {
      DataWriterUtil.convertTimeElementToString(-1)
    }
  }

  "A date time element" should "convert properly to a string" in {

  }

  "When processing an entry it" should "correctly process a double" in {
    assert(DataWriterUtil.processEntry(null, 2456909.098) == "2456909.098")

  }

  it should "return an empty string if is null" in {
    assert(DataWriterUtil.processEntry(null, null) == "")
  }

  it should "return an empty string if it is infinity" in {
    assert(DataWriterUtil.processEntry(null, DataWriterUtil.DoubleInfinityString) == "")
  }

  it should "correctly process time" in {
    DataWriterUtil.TimeFormatStrings.foreach(time =>
      assert(DataWriterUtil.processEntry(Column(0,"test","test",ColumnFormat(time, 0, 2),null,0),"3670") == "01:01:10")
    )
  }

  it should "correctly process a random string" in {
    assert(DataWriterUtil.processEntry(null, "fdas") == "fdas")
  }

  it should "correctly process percentages" in {
    val column = Column(0,"test","test",ColumnFormat(DataWriterUtil.PercentFormat, 1, 2), null, 0)
    assert(DataWriterUtil.processEntry(column,"0.496") == "49.60%")
  }

  it should "correctly process date time" in {
    val column = Column(0,"test","test",ColumnFormat(DataWriterUtil.PercentFormat, 1, 2), null, 0)
    assert(DataWriterUtil.processEntry(column, LocalDateTime.now()) != null)
  }

  }
