package com.kaszub.parso

import java.time.{LocalDate, LocalDateTime}

/**
  * A class to store all the sas7bdat file metadata.
  *
  * @param u64 The flag of the 64-bit version of SAS in which the sas7bdat file was created;
  *            false means the 32-bit version, true means the 64-bit version.
  * @param compressionMethod Compression method used for the sas7bdat file.
  * @param endianness The bytes sequence; 1 sets the little-endian sequence (Intel), 0 sets big-endian.
  * @param encoding The name of the sas7bdat character encoding.
  * @param sessionEncoding The name of the sas7bdat character encoding.
  * @param name The name of the sas7bdat file table.
  * @param fileType The type of the sas7bdat file.
  * @param dateCreated The date of the sas7bdat file creation.
  * @param dateModified The date of the last modification of the sas7bdat file.
  * @param sasRelease The version of SAS in which the sas7bdat was created.
  * @param serverType The version of the server on which the sas7bdat was created.
  * @param osName The name of the OS in which the sas7bdat file was created.
  * @param osType The version of the OS in which the sas7bdat file was created.
  * @param headerLength The number of bytes the sas7bdat file metadata takes.
  * @param pageLength The number of bytes each page of the sas7bdat file takes.
  * @param pageCount The number of pages in the sas7bdat file.
  * @param rowLength The number of bytes every row of the table takes.
  * @param rowCount The number of rows in the table.
  * @param mixPageRowCount The number of rows on the page containing both data and metadata.
  * @param columnsCount The number of columns in the table.
  *
  */
case class SasFileProperties (isU64 : Boolean, compressionMethod : String, endianness : Int, encoding : String,
                              sessionEncoding : String, name : String, fileType : String, dateCreated : LocalDateTime,
                              dateModified : LocalDateTime, sasRelease : String, serverType : String, osName : String,
                              osType : String, headerLength : Int, pageLength : Int, pageCount : Long, rowLength : Long,
                              rowCount : Long, mixPageRowCount : Long, columnsCount : Long) {

  def isCompressed: Boolean = compressionMethod != null

}
