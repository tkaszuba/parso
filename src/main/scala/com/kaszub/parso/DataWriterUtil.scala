package com.kaszub.parso

import java.io.IOException
import java.text.DecimalFormat
import java.time.{LocalDate, LocalDateTime}
import java.util.Locale

/**
  * A helper class to allow re-use formatted values from sas7bdat file.
  */
final object DataWriterUtil {

  /**
    * The number of digits starting from the first non-zero value, used to round doubles.
    */
  private val ACCURACY = 15

  /**
    * The class name of array of byte.
    */
  private val BYTE_ARRAY_CLASS_NAME = Seq[Byte](0).getClass.getName

  /**
    * Encoding used to convert byte arrays to string.
    */
  private val ENCODING = "CP1252"

  /**
    * If the number of digits in a double value exceeds a given constant, it rounds off.
    */
  private val ROUNDING_LENGTH = 13

  /**
    * The constant to check whether or not a string containing double stores infinity.
    */
  private val DOUBLE_INFINITY_STRING = "Infinity"

  /**
    * The format to output hours in the CSV format.
    */
  private val HOURS_OUTPUT_FORMAT = "%02d"

  /**
    * The format to output minutes in the CSV format.
    */
  private val MINUTES_OUTPUT_FORMAT = "%02d"

  /**
    * The format to output seconds in the CSV format.
    */
  private val SECONDS_OUTPUT_FORMAT = "%02d"

  /**
    * The delimiter between hours and minutes, minutes and seconds in the CSV file.
    */
  private val TIME_DELIMETER = ":"

  import java.util

  /**
    * The date formats to store the hour, minutes, seconds, and milliseconds. Appear in the data of
    * the {@link com.kaszub.parso.impl.FormatAndLabelSubheader} subheader
    * and are stored in {@link Column#format}.
    */
  private val TIME_FORMAT_STRINGS = Seq("TIME", "HHMM")

  /**
    * The format to store the percentage values. Appear in the data of
    * the {@link com.kaszub.parso.impl.SasFileParser.FormatAndLabelSubheader} subheader
    * and are stored in {@link Column#format}.
    */
  private val PERCENT_FORMAT = "PERCENT"

  /**
    * The number of seconds in a minute.
    */
  private val SECONDS_IN_MINUTE = 60

  /**
    * The number of minutes in an hour.
    */
  private val MINUTES_IN_HOUR = 60

  /**
    * The locale for dates in output row.
    */
  private val DEFAULT_LOCALE = Locale.getDefault

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
  private val DATE_OUTPUT_FORMAT_STRINGS = Map(
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
  @throws[IOException]
  private def processEntry(column: Column, entry: Any, locale: Locale): String = {

    entry match {
      case entry: Double => convertDoubleElementToString(entry)
      case entry: LocalDateTime => convertDateElementToString(entry, column.format.name, locale)
      case entry: String => {
        if (entry.contains(DOUBLE_INFINITY_STRING))
          ""
        else if (TIME_FORMAT_STRINGS contains column.format.name)
          convertTimeElementToString(entry.toLong) //Potential error if entry is not a long
        else if (PERCENT_FORMAT eq column.format.name)
          convertPercentElementToString(entry, column.format)
        else
          entry.toString()
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

  private def convertDateElementToString(currentDate: LocalDateTime, format: String, locale: Locale) : String = {
    return ???
  /*    import java.text.SimpleDateFormat
        import java.util.TimeZone
        var dateFormat: SimpleDateFormat = null
        var valueToPrint: String = ""
        dateFormat = new SimpleDateFormat(DATE_OUTPUT_FORMAT_STRINGS.get(format), locale)
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
        if (currentDate.getTime ne 0)  { valueToPrint = dateFormat.format(currentDate.getTime)
  */
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
  //TODO: Refactor
  private def convertTimeElementToString(secondsFromMidnight: Long): String = {
    ???
    /*return String.format(HOURS_OUTPUT_FORMAT, secondsFromMidnight / SECONDS_IN_MINUTE / MINUTES_IN_HOUR) +
      TIME_DELIMETER +
      String.format(MINUTES_OUTPUT_FORMAT, secondsFromMidnight / SECONDS_IN_MINUTE % MINUTES_IN_HOUR) +
      TIME_DELIMETER +
      String.format(SECONDS_OUTPUT_FORMAT, secondsFromMidnight % SECONDS_IN_MINUTE)*/
  }

  /**
    * The function to convert a double value into a string. If the text presentation of the double is longer
    * than {@link DataWriterUtil#ROUNDING_LENGTH}, the rounded off value of the double includes
    * the {@link DataWriterUtil#ACCURACY} number of digits from the first non-zero value.
    *
    * @param value the input numeric value to convert.
    * @return the string with the text presentation of the input numeric value.
    */
  //TODO: Refactor
  private def convertDoubleElementToString(value: Double): String = {
    var valueToPrint = String.valueOf(value)

    if (valueToPrint.length > ROUNDING_LENGTH) {
      val lengthBeforeDot = Math.ceil(Math.log10(Math.abs(value))).toInt
      val bigDecimal = BigDecimal(value).setScale(ACCURACY - lengthBeforeDot, BigDecimal.RoundingMode.HALF_UP)
      valueToPrint = String.valueOf(bigDecimal.doubleValue)
    }

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
      case value : Long => value.toDouble
      case value : Int => value.toDouble
      case value : Double => value
      case _ => throw new NumberFormatException(s"The passed value is not a double")
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

}
