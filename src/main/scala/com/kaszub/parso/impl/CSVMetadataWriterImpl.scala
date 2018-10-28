package com.kaszub.parso.impl

import java.io.Writer
import java.io.IOException

import com.kaszub.parso.{CSVMetadataWriter, CSVUtil, Column, SasFileProperties}

/**
  * This is a class to export the sas7bdat file metadata into the CSV format.
  */
//TODO: Side Effects everywhere, refactor with the other csv data writer
case class CSVMetadataWriterImpl(writer: Writer,
                                 delimiter: String = CSVUtil.Defaults.Delimiter,
                                 endline: String = CSVUtil.Defaults.Endline) extends CSVMetadataWriter {
  /**
    * The id column header for metadata.
    */
  private val ColumnHeadingId = "Number"

  /**
    * The name column header for metadata.
    */
  private val ColumnHeadingName = "Name"

  /**
    * The type column header for metadata.
    */
  private val ColumnHeadingType = "Type"

  /**
    * The data length column header for metadata.
    */
  private val ColumnHeadingDataLength = "Data length"

  /**
    * The format column header for metadata.
    */
  private val ColumnHeadingFormat = "Format"

  /**
    * The label column header for metadata.
    */
  private val ColumnHeadingLabel = "Label"

  /**
    * Constant containing Number type name.
    */
  private val JavaNumberClassName = "java.lang.Number"

  /**
    * Constant containing String type name.
    */
  private val JavaStringClassName = "java.lang.String"

  /**
    * Representation of Number type in metadata.
    */
  private val OutputNumberTypeName = "Numeric"

  /**
    * Representation of String type in metadata.
    */
  private val OutputStringTypeName = "Character"

  /**
    * The method to export a parsed sas7bdat file metadata (stored as an object of the {@link SasFileReaderImpl} class)
    * using {@link CSVMetadataWriterImpl#writer}.
    *
    * @param columns the { @link Column} class variables list that stores columns description from the sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeMetadata(columns: Seq[Column]): Unit = {

    writer.write(ColumnHeadingId)
    writer.write(delimiter)
    writer.write(ColumnHeadingName)
    writer.write(delimiter)
    writer.write(ColumnHeadingType)
    writer.write(delimiter)
    writer.write(ColumnHeadingDataLength)
    writer.write(delimiter)
    writer.write(ColumnHeadingFormat)
    writer.write(delimiter)
    writer.write(ColumnHeadingLabel)

    columns.foreach(column => {
      writer.write(String.valueOf(column.id))
      writer.write(delimiter)
      CSVUtil.checkSurroundByQuotesAndWrite(writer, delimiter, column.name.getOrElse(""))
      writer.write(delimiter)
      writer.write(
        column._type match {
          case Some(value) => value.getName.
            replace(JavaNumberClassName, OutputNumberTypeName).
            replace(JavaStringClassName, OutputStringTypeName)
          case _ => ""
        }
      )
      writer.write(delimiter)
      writer.write(String.valueOf(column.length))
      writer.write(delimiter)
      if (!column.format.isEmpty)
        CSVUtil.checkSurroundByQuotesAndWrite(writer, delimiter,
          column.format.name match {
            case Left(value) => value
            case _ => ""
          }
        )
      writer.write(delimiter)
      CSVUtil.checkSurroundByQuotesAndWrite(writer, delimiter,
        column.label match {
          case Left(value) => value
          case _ => ""
        })
      writer.write(endline)
    })

    writer.flush
  }

  /**
    * The method to output the sas7bdat file properties.
    *
    * @param sasFileProperties the variable with sas file properties data.
    * @throws IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeSasFileProperties(sasFileProperties: SasFileProperties): Unit = {

    writeProperties("Bitness: ", if (sasFileProperties.isU64) "x64" else "x86")
    writeProperties("Compressed: ", sasFileProperties.compressionMethod)
    writeProperties("Endianness: ",
      if (sasFileProperties.endianness == 1) "LITTLE_ENDIANNESS" else "BIG_ENDIANNESS")
    writeProperties("Name: ", sasFileProperties.name)
    writeProperties("File type: ", sasFileProperties.fileType)
    writeProperties("Date created: ", sasFileProperties.dateCreated)
    writeProperties("Date modified: ", sasFileProperties.dateModified)
    writeProperties("SAS release: ", sasFileProperties.sasRelease)
    writeProperties("SAS server type: ", sasFileProperties.serverType)
    writeProperties("OS name: ", sasFileProperties.osName)
    writeProperties("OS type: ", sasFileProperties.osType)
    writeProperties("Header Length: ", sasFileProperties.headerLength)
    writeProperties("Page Length: ", sasFileProperties.pageLength)
    writeProperties("Page Count: ", sasFileProperties.pageCount)
    writeProperties("Row Length: ", sasFileProperties.rowLength)
    writeProperties("Row Count: ", sasFileProperties.rowCount)
    writeProperties("Mix Page Row Count: ", sasFileProperties.mixPageRowCount)
    writeProperties("Columns Count: ", sasFileProperties.columnsCount)

    writer.flush
  }

  /**
    * The method to output string containing information about passed property using writer.
    *
    * @param propertyName the string containing name of a property.
    * @param property     a property value.
    * @throws IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  private def writeProperties(propertyName: String, property: Any): Unit =
    writer.write(propertyName + String.valueOf(property) + endline)
}
