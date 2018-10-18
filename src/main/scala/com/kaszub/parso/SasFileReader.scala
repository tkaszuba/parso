package com.kaszub.parso

import java.io.IOException

/**
  * Main trait for working with the library.
  */
trait SasFileReader {

  /**
    * The function to get the {@link Column} list from {@link SasFileReader}.
    *
    * @return a list of columns.
    */
  def getColumns: Seq[Column]

  /**
    * The function to get the {@link Column} list from {@link SasFileReader}
    * according to the columnNames.
    *
    * @param columnNames list of column names which should be returned.
    * @return a list of columns.
    */
  def getColumns(columnNames: Seq[String]): Seq[Column]

  /**
    * Reads all rows from the sas7bdat file.
    *
    * @return an array of array objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    */
  def readAll: Seq[Seq[Any]]

  /**
    * Reads all rows from the sas7bdat file. For each row, only the columns defined in the list are read.
    *
    * @param columnNames list of column names which should be processed.
    * @return an array of array objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    */
  def readAll(columnNames: Seq[String]): Seq[Seq[Any]]

  /**
    * Reads rows one by one from the sas7bdat file.
    *
    * @return an array of objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    * @throws IOException if reading input stream is impossible.
    */
  @throws[IOException]
  def readNext: Seq[Any]

  /**
    * Reads rows one by one from the sas7bdat file. For each row, only the columns defined in the list are read.
    *
    * @param columnNames list of column names which should be processed.
    * @return an array of objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    * @throws IOException if reading input stream is impossible.
    */
  @throws[IOException]
  def readNext(columnNames: Seq[String]): Seq[Any]

  /**
    * The function to get sas file properties.
    *
    * @return the object of the {@link SasFileProperties} class that stores file metadata.
    */
  def getSasFileProperties: SasFileProperties
}
