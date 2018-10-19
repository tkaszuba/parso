package com.kaszub.parso

import java.io.IOException
import java.text.DecimalFormat
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.util.Locale

import scala.io.Source
import scala.util.Try

/**
  * A helper class to allow re-use formatted values from sas7bdat file.
  */
final object DataWriterUtil {

  /**
    * The number of digits starting from the first non-zero value, used to round doubles.
    */
  private val MaxDoublePrecision = 15

  /**
    * Encoding used to convert byte arrays to string.
    */
  private val Encoding = "CP1252"

  /**
    * If the number of digits in a double value exceeds a given constant, it rounds off.
    */
  private val DoubleRoundingLength = 13

  /**
    * The constant to check whether or not a string containing double stores infinity.
    */
  private val DoubleInfinityString = "Infinity"

  /**
    * The format to output hours in the CSV format.
    */
  private val HoursOutputFormat = "%02d"

  /**
    * The format to output minutes in the CSV format.
    */
  private val MinutesOutputFormat = "%02d"

  /**
    * The format to output seconds in the CSV format.
    */
  private val SecondsOutputFormat = "%02d"

  /**
    * The delimiter between hours and minutes, minutes and seconds in the CSV file.
    */
  private val TimeDelimiter = ":"

  /**
    * The date formats to store the hour, minutes, seconds, and milliseconds. Appear in the data of
    * the {@link com.kaszub.parso.impl.FormatAndLabelSubheader} subheader
    * and are stored in {@link Column#format}.
    */
  private val TimeFormatStrings = Seq("TIME", "HHMM")

  /**
    * The format to store the percentage values. Appear in the data of
    * the {@link com.kaszub.parso.impl.SasFileParser.FormatAndLabelSubheader} subheader
    * and are stored in {@link Column#format}.
    */
  private val PercentFormat = "PERCENT"

  /**
    * The number of seconds in a minute.
    */
  private val SecondsInMinute = 60

  /**
    * The number of minutes in an hour.
    */
  private val MinutesInHour = 60

  /**
    * The locale for dates in output row.
    */
  private val DefaultLocale = Locale.getDefault

  /**
    * The format to output percentage values in the CSV format. This format is used
    * when {@link ColumnFormat#precision} does not contain the accuracy of rounding.
    */
  //TODO: This is actually the Scale not Precision, see if it can be renamed
  private val ZeroPrecisionFormat ="0%"
  private val NonZeroPrecisionFormat = ZeroPrecisionFormat + "."
  /**
    * These are sas7bdat format references to {@link java.text.SimpleDateFormat} date formats.
    * <p>
    * UNSUPPORTED FORMATS:
    * DTYYQC, PDJULG, PDJULI, QTR, QTRR, WEEKU, WEEKV, WEEKW,
    * YYQ, YYQC, YYQD, YYQN, YYQP, YYQS, YYQR, YYQRC, YYQRD, YYQRN, YYQRP, YYQRS
    */
  private val DateOutputFormatStrings = Map(
    "B8601DA" -> "yyyyMMdd",
    "E8601DA" -> "yyyy-MM-dd",
    "DATE" -> "ddMMMyyyy",
    "DAY" -> "dd",
    "DDMMYY" -> "dd/MM/yyyy",
    "DDMMYYB" -> "dd MM yyyy",
    "DDMMYYC" -> "dd:MM:yyyy",
    "DDMMYYD" -> "dd-MM-yyyy",
    "DDMMYYN" -> "ddMMyyyy",
    "DDMMYYP" -> "dd.MM.yyyy",
    "DDMMYYS" -> "dd/MM/yyyy",
    "JULDAY" -> "D",
    "JULIAN" -> "yyyyD",
    "MMDDYY" -> "MM/dd/yyyy",
    "MMDDYYB" -> "MM dd yyyy",
    "MMDDYYC" -> "MM:dd:yyyy",
    "MMDDYYD" -> "MM-dd-yyyy",
    "MMDDYYN" -> "MMddyyyy",
    "MMDDYYP" -> "MM.dd.yyyy",
    "MMDDYYS" -> "MM/dd/yyyy",
    "MMYY" -> "MM'M'yyyy",
    "MMYYC" -> "MM:yyyy",
    "MMYYD" -> "MM-yyyy",
    "MMYYN" -> "MMyyyy",
    "MMYYP" -> "MM.yyyy",
    "MMYYS" -> "MM/yyyy",
    "MONNAME" -> "MMMM",
    "MONTH" -> "M",
    "MONYY" -> "MMMyyyy",
    "WEEKDATE" -> "EEEE, MMMM dd, yyyy",
    "WEEKDATX" -> "EEEE, dd MMMM, yyyy",
    "WEEKDAY" -> "u",
    "DOWNAME" -> "EEEE",
    "WORDDATE" -> "MMMM d, yyyy",
    "WORDDATX" -> "d MMMM yyyy",
    "YYMM" -> "yyyy'M'MM",
    "YYMMC" -> "yyyy:MM",
    "YYMMD" -> "yyyy-MM",
    "YYMMN" -> "yyyyMM",
    "YYMMP" -> "yyyy.MM",
    "YYMMS" -> "yyyy/MM",
    "YYMMDD" -> "yyyy-MM-dd",
    "YYMMDDB" -> "yyyy MM dd",
    "YYMMDDC" -> "yyyy:MM:dd",
    "YYMMDDD" -> "yyyy-MM-dd",
    "YYMMDDN" -> "yyyyMMdd",
    "YYMMDDP" -> "yyyy.MM.dd",
    "YYMMDDS" -> "yyyy/MM/dd",
    "YYMON" -> "yyyyMMM",
    "YEAR" -> "yyyy",

    /* datetime formats */
    "B8601DN" -> "yyyyMMdd",
    "B8601DT" -> "yyyyMMdd'T'HHmmssSSS",
    "B8601DX" -> "yyyyMMdd'T'HHmmssZ",
    "B8601DZ" -> "yyyyMMdd'T'HHmmssZ",
    "B8601LX" -> "yyyyMMdd'T'HHmmssZ",
    "E8601DN" -> "yyyy-MM-dd",
    "E8601DT" -> "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "E8601DX" -> "yyyy-MM-dd'T'HH:mm:ssZ",
    "E8601DZ" -> "yyyy-MM-dd'T'HH:mm:ssZ",
    "E8601LX" -> "yyyy-MM-dd'T'HH:mm:ssZ",
    "DATEAMPM" -> "ddMMMyyyy:HH:mm:ss.SS a",
    "DATETIME" -> "ddMMMyyyy:HH:mm:ss.SS",
    "DTDATE" -> "ddMMMyyyy",
    "DTMONYY" -> "MMMyyyy",
    "DTWKDATX" -> "EEEE, dd MMMM, yyyy",
    "DTYEAR" -> "yyyy",
    "MDYAMPM" -> "MM/dd/yyyy H:mm a",
    "TOD" -> "HH:mm:ss.SS"
  )

  /**
    * Checks current entry type and returns its string representation.
    *
    * @param column current processing column.
    * @param entry  current processing entry.
    * @param locale the locale for parsing date elements.
    * @return a string representation of current processing entry.
    * @throws IOException appears if the output into writer is impossible.
    */
  def processEntry(column: Column, entry: Any, locale: Locale = DefaultLocale): String = {

    entry match {
      case entry: Double => convertDoubleElementToString(entry)
      case entry: ZonedDateTime => convertDateTimeElementToString(entry, column.format.name, locale)
      case entry: String => {
        if (entry.contains(DoubleInfinityString))
          ""
        else if (column != null && (TimeFormatStrings contains column.format.name))
          convertTimeElementToString(entry)
        else if (column != null && PercentFormat == column.format.name)
          convertPercentElementToString(entry, column.format)
        else
          entry
      }
      case _ => ""
    }
  }

  /**
    * The function to convert a date into a string according to the format used.
    *
    * @param currentDate the date to convert.
    * @param format      the string with the format that must belong to the set of
    *                    {@link DataWriterUtil#DATE_OUTPUT_FORMAT_STRINGS} mapping keys.
    * @param locale the locale for parsing date.
    * @return the string that corresponds to the date in the format used.
    */
  def convertDateTimeElementToString(currentDate: ZonedDateTime, format: String, locale: Locale = DefaultLocale) : String = {
    assert(currentDate != null, "The date time can't be null")
    assert(format != null, "The date time format can't be null")
    assert(DateOutputFormatStrings.contains(format), s"The passed date time format ${format} is not supported")

    currentDate.format(DateTimeFormatter.ofPattern(DateOutputFormatStrings(format), locale))
  }

  private def convertTimeElementToString(secondsFromMidnight: String): String = {
    assert(Try(secondsFromMidnight.toLong).isSuccess, "The passed value is not convertible to a long")
    convertTimeElementToString(secondsFromMidnight.toLong)
  }

  /**
    * The function to convert time without a date (hour, minute, second) from the sas7bdat file format
    * (which is the number of seconds elapsed from the midnight) into a string of the format set by the constants:
    * {@link DataWriterUtil#HOURS_OUTPUT_FORMAT}, {@link DataWriterUtil#MINUTES_OUTPUT_FORMAT},
    * {@link DataWriterUtil#SECONDS_OUTPUT_FORMAT}, and {@link DataWriterUtil#TIME_DELIMETER}.
    *
    * @param secondsFromMidnight the number of seconds elapsed from the midnight.
    * @return the string of time in the format set by constants.
    */
  def convertTimeElementToString(secondsFromMidnight: Long): String = {
    assert(secondsFromMidnight >= 0, "seconds from midnight must be greater or equal than zero")

    HoursOutputFormat.format(secondsFromMidnight / SecondsInMinute / MinutesInHour) +
    TimeDelimiter +
    MinutesOutputFormat.format(secondsFromMidnight / SecondsInMinute % MinutesInHour) +
    TimeDelimiter +
    SecondsOutputFormat.format(secondsFromMidnight %  SecondsInMinute)
  }

  /**
    * The function to convert a double value into a string. If the text presentation of the double is longer
    * than {@link DataWriterUtil#ROUNDING_LENGTH}, the rounded off value of the double includes
    * the {@link DataWriterUtil#ACCURACY} number of digits from the first non-zero value.
    *
    * @param value the input numeric value to convert.
    * @return the string with the text presentation of the input numeric value.
    */
  def convertDoubleElementToString(value: Double): String = {
    val valueToPrint = String.valueOf(value)

    //Strange to use the length of a string to count the precision instead of a BigDecimal, performance?
    if (valueToPrint.length > DoubleRoundingLength) {
      val lengthBeforeDot = Math.ceil(Math.log10(Math.abs(value))).toInt
      val bigDecimal = BigDecimal(value).setScale(MaxDoublePrecision - lengthBeforeDot, BigDecimal.RoundingMode.HALF_UP)
      String.valueOf(bigDecimal.doubleValue)
    }
    else
      trimZerosFromEnd(valueToPrint)
  }

  /**
    * The function to convert a percent element into a string. The accuracy of rounding
    * is stored in {@link Column#format}.
    *
    * @param value        the input numeric value to convert.
    * @param columnFormat the column format containing the precision of rounding the converted value.
    * @return the string with the text presentation of the input numeric value.
    */
  def convertPercentElementToString(value: Any, columnFormat: ColumnFormat): String = {
    val doubleValue = value match {
      case value : String => value.toDouble
      case value : Long => value.toDouble
      case value : Int => value.toDouble
      case value : Double => value
      case _ => throw new NumberFormatException("The passed value is not a double")
    }

    val df =
      if (columnFormat.precision > 0)
        new DecimalFormat(NonZeroPrecisionFormat.padTo(NonZeroPrecisionFormat.length + columnFormat.precision, '0'))
      else
        new DecimalFormat(ZeroPrecisionFormat)

    df.format(doubleValue)
  }

  /**
    * The function to remove trailing zeros from the decimal part of the numerals represented by a string.
    * If there are no digits after the point, the point is deleted as well.
    *
    * @param string the input string trailing zeros.
    * @return the string without trailing zeros.
    */
  def trimZerosFromEnd(string: String): String =
    string.replaceAll("0*$", "").replaceAll("\\.$", "")

  /**
    * The method to convert the Objects array that stores data from the sas7bdat file to list of string.
    *
    * @param columns the {@link Column} class variables list that stores columns description from the sas7bdat file.
    * @param row    the Objects arrays that stores data from the sas7bdat file.
    * @param locale the locale for parsing date elements.
    * @return list of String objects that represent data from sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  def getRowValues(columns: Seq[Column], row: Seq[Any], locale: Locale = DefaultLocale): Seq[String] =
   columns.zipWithIndex.map(el => getValue(el._1, row(el._2), locale))

  /**
    * The method to convert the Object that stores data from the sas7bdat file cell to string.
    *
    * @param column the {@link Column} class variable that stores current processing column.
    * @param entry  the Object that stores data from the cell of sas7bdat file.
    * @param locale the locale for parsing date elements.
    * @return a string representation of current processing entry.
    * @throws IOException appears if the output into writer is impossible.
    */
  def getValue(column: Column, entry: Any, locale: Locale = DefaultLocale): String = {

    entry match {
      case null => null
      case entry : Seq[Byte] => Source.fromBytes(entry.toArray, Encoding).mkString
      case entry => processEntry (column, entry, locale)
    }
  }

}
