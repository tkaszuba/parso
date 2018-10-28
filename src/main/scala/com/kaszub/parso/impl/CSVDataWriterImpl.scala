package com.kaszub.parso.impl

import java.io.Writer
import java.util.Locale
import java.io.IOException

import com.kaszub.parso.{CSVDataWriter, CSVUtil, Column, DataWriterUtil}

case class CSVDataWriterImpl(writer: Writer,
                             delimiter: String = CSVUtil.Defaults.Delimiter,
                             endline: String = CSVUtil.Defaults.Endline,
                             locale: Locale = CSVUtil.Defaults.Locale) extends CSVDataWriter {

  /**
    * The method to export a row from sas7bdat file (stored as an object of the {@link SasFileReaderImpl} class)
    * using {@link CSVDataWriterImpl#writer}.
    *
    * @param columns the { @link Column} class variables list that stores columns description from the sas7bdat file.
    * @param row the Objects arrays that stores data from the sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeRow(columns: Seq[Column], row: Seq[Any]): Unit = {
    if (row == null) return

    val valuesToPrint = DataWriterUtil.getRowValues(columns, row, locale)

    for (i <- 0 to columns.size){
      writer.write(CSVUtil.checkSurroundByQuotes(delimiter, valuesToPrint(i)))
      if (i != columns.size - 1)
        writer.write(delimiter)
    }

    writer.write(endline)
    writer.flush()
  }

  /**
    * The method to export a parsed sas7bdat file (stored as an object of the {@link SasFileReaderImpl} class)
    * using {@link CSVDataWriterImpl#writer}.
    *
    * @param columns the { @link Column} class variables list that stores columns description from the sas7bdat file.
    * @param rows the Objects arrays array that stores data from the sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeRowsArray(columns: Seq[Column], rows: Seq[Seq[Any]]): Unit =
    rows.foreach(row => if (row != null) writeRow(columns, row) else return)

  /**
    * The method to output the column names using the {@link CSVDataWriterImpl#delimiter} delimiter
    * using {@link CSVDataWriterImpl#writer}.
    *
    * @param columns the list of column names.
    * @throws IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  //TODO: write Column names and writer row should be one method
  def writeColumnNames(columns: Seq[Column]): Unit = {

    for (i <- 0 to columns.size){
      CSVUtil.checkSurroundByQuotesAndWrite(writer, delimiter, columns(i).name.getOrElse(""))
      if (i != columns.size - 1)
        writer.write(delimiter)
    }

    writer.write(endline)
    writer.flush()
  }
}
