package com.kaszub.parso

import java.io.IOException

/**
  * Trait for exporting data from sas7bdat file to csv.
  */
trait CVSDataWriter {

  /**
    * The method to export a row from sas7bdat file (stored as an object of the
    * {@link com.kaszub.parso.SasFileReaderImpl} class) using writer.
    *
    * @param columns the {@link Column} class variables list that stores columns description from the sas7bdat file.
    * @param row the Objects arrays that stores data from the sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeRow(columns: Seq[Column], row: Seq[Any]): Unit

  /**
    * The method to export a parsed sas7bdat file (stored as an object of the
    * {@link com.tkaszuba.parso.SasFileReaderImpl} class) using writer.
    *
    * @param columns the { @link Column} class variables list that stores columns description from the sas7bdat file.
    * @param rows the Objects arrays array that stores data from the sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeRowsArray(columns: Seq[Column], rows: Seq[Seq[Any]]): Unit

  /**
    * The method to output the column names using the delimiter using writer.
    *
    * @param columns the list of column names.
    * @throws IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeColumnNames(columns: Seq[Column]): Unit
}
