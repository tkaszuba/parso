package com.kaszub.parso

import java.time.ZonedDateTime

import org.scalatest.FlatSpec

class DataWriterUtilSuite extends FlatSpec {

  val Infinity = TestUtils.getConst[String](DataWriterUtil,"DoubleInfinityString")

  val DateFormats = TestUtils.getConst[Map[String,String]](DataWriterUtil,"DateOutputFormatStrings")
  val PercentFormat = TestUtils.getConst[String](DataWriterUtil,"PercentFormat")
  val TimeFormats = TestUtils.getConst[Seq[String]](DataWriterUtil,"TimeFormatStrings")

  val ColumnFormatNoPrecision = ColumnFormat(Left("test"), 1, 0)
  val ColumnFormatWithPrecision = ColumnFormat(Left("test"), 1, 2)
  val ColumnFormatPercentage = ColumnFormat(Left(PercentFormat), 1, 2)
  val ColumnFormatTime = ColumnFormat(Left(TimeFormats.head), 1, 2)
  val ColumnFormatDate = ColumnFormat(Left(DateFormats.keys.head), 1, 2)

  "A percent element when converting to a string" should "convert properly with no column format precision" in {
    assert(DataWriterUtil.convertPercentElementToString(2.0, ColumnFormatNoPrecision) === "200%")
    assert(DataWriterUtil.convertPercentElementToString(2, ColumnFormatNoPrecision) === "200%")
    assert(DataWriterUtil.convertPercentElementToString(2L, ColumnFormatNoPrecision) === "200%")
    assert(DataWriterUtil.convertPercentElementToString(1.5, ColumnFormatNoPrecision) === "150%")
    assert(DataWriterUtil.convertPercentElementToString(0.997, ColumnFormatNoPrecision) === "100%")
    assert(DataWriterUtil.convertPercentElementToString(0.987, ColumnFormatNoPrecision) === "99%")
    assert(DataWriterUtil.convertPercentElementToString(0.496, ColumnFormatNoPrecision) === "50%")
  }

  it should "convert properly with column format precision" in {
    assert(DataWriterUtil.convertPercentElementToString(2.0, ColumnFormatWithPrecision) === "200.00%")
    assert(DataWriterUtil.convertPercentElementToString(2, ColumnFormatWithPrecision) === "200.00%")
    assert(DataWriterUtil.convertPercentElementToString(2L, ColumnFormatWithPrecision) === "200.00%")
    assert(DataWriterUtil.convertPercentElementToString(1.5, ColumnFormatWithPrecision) === "150.00%")
    assert(DataWriterUtil.convertPercentElementToString(0.997, ColumnFormatWithPrecision) === "99.70%")
    assert(DataWriterUtil.convertPercentElementToString(0.987, ColumnFormatWithPrecision) === "98.70%")
    assert(DataWriterUtil.convertPercentElementToString(0.496, ColumnFormatWithPrecision) === "49.60%")
  }

  it should "produce a NumberFormatException when passed something other then a long, int or double" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString({}, ColumnFormatNoPrecision)
    }
  }

  it should "produce a NumberFormatException when passed a null" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString(null, ColumnFormatNoPrecision)
    }
  }

  it should "produce a NumberFormatException when passed a non convertible string" in {
    intercept[NumberFormatException] {
      DataWriterUtil.convertPercentElementToString("a", ColumnFormatNoPrecision)
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
      (0L, "00:00:00"),
      (10L, "00:00:10"),
      (60L, "00:01:00"),
      (70L, "00:01:10"),
      (3600L, "01:00:00"),
      (3670L, "01:01:10")
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
    DateFormats.foreach(key =>
      assert(DataWriterUtil.convertDateTimeElementToString(ZonedDateTime.now, key._1) != null)
    )
  }

  it should "produce an AssertionException when the date is null" in {
    intercept[AssertionError] {
      DataWriterUtil.convertDateTimeElementToString(null, "32312")
    }
  }

  it should "produce an AssertionException when the date format is null" in {
    intercept[AssertionError] {
      DataWriterUtil.convertDateTimeElementToString(ZonedDateTime.now, null)
    }
  }

  it should "produce an AssertionException when the date format can't be found" in {
    intercept[AssertionError] {
      DataWriterUtil.convertDateTimeElementToString(ZonedDateTime.now, "fdasfas!")
    }
  }

  "When processing an entry it" should "correctly process a double" in {
    assert(DataWriterUtil.processEntry(null, 2456909.098) == "2456909.098")
  }

  it should "return an empty string if is null" in {
    assert(DataWriterUtil.processEntry(null, null) == "")
  }

  it should "return an empty string if it is infinity" in {
    assert(DataWriterUtil.processEntry(null, Infinity) == "")
  }

  it should "correctly process time" in {
    TimeFormats.foreach(time =>
      assert(DataWriterUtil.processEntry(Column(Some(0), Some("test"), ColumnLabel(Left("test")), ColumnFormat(Left(time), 1, 2), null, Some(0)), "3670") == "01:01:10")
    )
  }

  it should "correctly process a random string" in {
    assert(DataWriterUtil.processEntry(null, "fdas") == "fdas")
  }

  it should "correctly process percentages" in {
    val column = Column(Some(0), Some("test"), ColumnLabel(Left("test")), ColumnFormatPercentage, null, Some(0))
    assert(DataWriterUtil.processEntry(column, "0.496") == "49.60%")
  }

  it should "correctly process date time" in {
    val column = Column(Some(0), Some("test"), ColumnLabel(Left("test")), ColumnFormatDate, null, Some(0))
    assert(DataWriterUtil.processEntry(column, ZonedDateTime.now()) != null)
  }

  "When getting the value it" should "correctly process an entry" in {
    val column = Column(Some(0), Some("test"), ColumnLabel(Left("test")), ColumnFormatPercentage, null, Some(0))
    assert(DataWriterUtil.getValue(column, "0.496") == "49.60%")
  }

  it should "return null if null is passed" in {
    assert(DataWriterUtil.getValue(null, null) == null)
  }

  it should "return a decoded string if an array of bytes is passed" in {
    //val bytes = Seq('±','²','³','´','µ').map(_.toByte)
    val bytes = Seq(177, 178, 179, 180, 181).map(_.toByte)

    assert(DataWriterUtil.getValue(null, bytes) == "±²³´µ")
  }

  "When getting the row values it" should "correctly return the entries" in {
    val columnMeta = Seq[Column](
      Column(Some(1), Some("Column1Name"), ColumnLabel(Left("Column1Label")), ColumnFormatPercentage, null, Some(0)),
      Column(Some(2), Some("Column2Name"), ColumnLabel(Left("Column2Label")), ColumnFormatTime, null, Some(0)),
      Column(Some(3), Some("Column3Name"), ColumnLabel(Left("Column3Label")), ColumnFormatDate, null, Some(0)),
      Column(Some(4), Some("Column4Name"), ColumnLabel(Left("Column4Label")), ColumnFormatNoPrecision, null, Some(0)),
      Column(Some(5), Some("Column5Name"), ColumnLabel(Left("Column5Label")), ColumnFormatWithPrecision, null, Some(0)),
      null
    )
    val row = Seq[Any](
      "0.496",
      "3670",
      ZonedDateTime.now,
      0.997,
      .997,
      Infinity
    )

    val values = DataWriterUtil.getRowValues(columnMeta, row)

    assert(values(0) == "49.60%")
    assert(values(1) == "01:01:10")
    assert(values(2) != "")
    assert(values(3) == "0.997")
    assert(values(4) == "0.997")
    assert(values(5) == "")
  }
}
