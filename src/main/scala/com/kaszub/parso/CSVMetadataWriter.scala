package com.kaszub.parso

import java.io.IOException

/**
  * Trait for exporting metadata from sas7bdat file to csv.
  */
trait CSVMetadataWriter {

  /**
    * The method to export a parsed sas7bdat file metadata using writer.
    *
    * @param columns the { @link Column} class variables list that stores columns description from the sas7bdat file.
    * @throws java.io.IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeMetadata(columns: Seq[Column]): Unit

  /**
    * The method to output the sas7bdat file properties.
    *
    * @param sasFileProperties the variable with sas file properties data.
    * @throws IOException appears if the output into writer is impossible.
    */
  @throws[IOException]
  def writeSasFileProperties(sasFileProperties : SasFileProperties): Unit

}
