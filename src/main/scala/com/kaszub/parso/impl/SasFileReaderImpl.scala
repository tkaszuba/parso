package com.kaszub.parso.impl

import java.io.{IOException, InputStream}

import com.kaszub.parso.{Column, SasFileProperties, SasFileReader}
import com.typesafe.scalalogging.Logger

/**
  * A class to read sas7bdat files transferred to the input stream and then to get metadata and file data.
  * This class is used as a wrapper for SasFileParser.
  */
class SasFileReaderImpl (private val sasFileParser: SasFileParser) extends SasFileReader{

  require(sasFileParser != null)

  /**
    * Object for writing logs.
    */
  private val logger = Logger[this.type]

  /**
    * The function to get the {@link Column} list from {@link SasFileParser}.
    *
    * @return a list of columns.
    */
  def getColumns: Seq[Column] = ??? //sasFileParser.getColumns

  /**
    * The function to get the {@link Column} list from {@link SasFileReader}
    * according to the columnNames.
    *
    * @param columnNames - list of column names that should be returned.
    * @return a list of columns.
    */
  def getColumns(columnNames: Seq[String]): Seq[Column] = ???

  /**
  * Reads all rows from the sas7bdat file. For each row, only the columns defined in the list are read.
    *
  * @param columnNames list of column names which should be processed.
  * @return an array of array objects whose elements can be objects of the following classes: double, long,
  * int, byte[], Date depending on the column they are in.
  */
  def readAll(columnNames : Seq[String]): Seq[Seq[Any]]  = ???

  /**
    * Reads all rows from the sas7bdat file.
    *
    * @return an array of array objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    */
  def readAll: Seq[Seq[Any]] = readAll(null)

  /**
    * Reads all rows from the sas7bdat file.
    *
    * @return an array of array objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    */
  @throws[IOException]
  def readNext: Seq[Any] = ??? //sasFileParser.readNext(null)

  /**
    * Reads all rows from the sas7bdat file. For each row, only the columns defined in the list are read.
    *
    * @param columnNames list of column names which should be processed.
    * @return an array of array objects whose elements can be objects of the following classes: double, long,
    *         int, byte[], Date depending on the column they are in.
    */
  @throws[IOException]
  def readNext(columnNames: Seq[String]): Seq[Any] = ??? //sasFileParser.readNext(columnNames)

  /**
    * The function to get sas file properties.
    *
    * @return the object of the { @link SasFileProperties} class that stores file metadata.
    */
  def getSasFileProperties: SasFileProperties = sasFileParser.getSasFileProperties
}

object SasFileReaderImpl {
  /**
    * Builds an object of the SasFileReaderImpl class from the file contained in the input stream.
    * Reads only metadata (properties and column information) of the sas7bdat file.
    *
    * @param inputStream - an input stream which should contain a correct sas7bdat file.
    */
  def apply(inputStream: InputStream) =
    new SasFileReaderImpl(SasFileParser().Builder().sasFileStream(inputStream).build)

  /**
    * Builds an object of the SasFileReaderImpl class from the file contained in the input stream with the encoding
    * defined in the 'encoding' variable.
    * Reads only metadata (properties and column information) of the sas7bdat file.
    *
    * @param inputStream - an input stream which should contain a correct sas7bdat file.
    * @param encoding    - the string containing the encoding to use in strings output
    */
  def apply(inputStream: InputStream, encoding: String) =
    new SasFileReaderImpl(SasFileParser().Builder().sasFileStream(inputStream).encoding(encoding).build)

  /**
    * Builds an object of the SasFileReaderImpl class from the file contained in the input stream with a flag of
    * the binary or string format of the data output.
    * Reads only metadata (properties and column information) of the sas7bdat file.
    *
    * @param inputStream - an input stream which should contain a correct sas7bdat file.
    * @param byteOutput  - the flag of data output in binary or string format
    */
  def apply(inputStream: InputStream, byteOutput: Boolean) =
    new SasFileReaderImpl(SasFileParser().Builder().sasFileStream(inputStream).byteOutput(byteOutput).build)

}
