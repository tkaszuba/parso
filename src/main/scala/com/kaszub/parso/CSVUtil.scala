package com.kaszub.parso

import java.io.{IOException, Writer}

object CSVUtil {

  object Defaults {
    /**
      * The delimiter to use in the CSV format.
      */
    val Delimiter = ","

    /**
      * The default endline for csv file.
      */
    val Endline = "\n"

    /**
      * The default locale for csv file.
      */
    val Locale = java.util.Locale.getDefault()
  }

  /**
    * The method to write a text represented by an array of bytes using writer.
    * If the text contains the delimiter, line breaks, tabulation characters, and double quotes, the text is stropped.
    *
    * @param writer      the variable to output data.
    * @param delimiter   if trimmedText contains this delimiter it will be stropped.
    * @param trimmedText the array of bytes that contains the text to output.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def checkSurroundByQuotesAndWrite(writer: Writer, delimiter: String, trimmedText: String): Unit =
    writer.write(checkSurroundByQuotes(delimiter, trimmedText))

  /**
    * The method to output a text represented by an array of bytes.
    * If the text contains the delimiter, line breaks, tabulation characters, and double quotes, the text is stropped.
    *
    * @param delimiter   if trimmedText contains this delimiter it will be stropped.
    * @param trimmedText the array of bytes that contains the text to output.
    * @return string represented by an array of bytes.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def checkSurroundByQuotes(delimiter: String, trimmedText: String): String = {
    ???
    /*val containsDelimiter = stringContainsItemFromList(trimmedText, delimiter, "\n", "\t", "\r", "\"")
    val trimmedTextWithoutQuotesDuplicates = trimmedText.replace("\"", "\"\"")
    if (containsDelimiter && trimmedTextWithoutQuotesDuplicates.length != 0) return "\"" + trimmedTextWithoutQuotesDuplicates + "\""
    trimmedTextWithoutQuotesDuplicates*/
  }

  /**
    * The method to check if string contains as a substring at least one string from list.
    *
    * @param inputString string which is checked for containing string from the list.
    * @param items       list of strings.
    * @return true if at least one of strings from the list is a substring of original string.
    */
  private def stringContainsItemFromList(inputString: String, items: String*): Boolean = {
    for (item <- items) {
      if (inputString.contains(item)) return true
    }
    false
  }

}
