package com.kaszub.parso

import java.time.{Instant, ZoneOffset}

import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

class SasFileReaderUnitTest extends FlatSpec {

  private val logger = Logger[this.type]

  private val DEFAULT_FILE_NAME = "sas7bdat//all_rand_normal.sas7bdat"
  private val COLON_COLUMN_IDS = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
  private val COLON_COLUMN_NAMES = Seq("sex", "age", "stage", "mmdx", "yydx", "surv_mm", "surv_yy",
    "status", "subsite", "year8594", "agegrp", "dx", "exit")
  private val COLON_COLUMN_FORMATS = Seq("", "", "", "", "", "", "", "", "", "", "", "MMDDYY10.", "MMDDYY10.")
  private val COLON_COLUMN_LABELS = Seq("Sex", "Age at diagnosis", "Clinical stage at diagnosis", "Month of diagnosis",
    "Year of diagnosis", "Survival time in months", "Survival time in years", "Vital status at last contact",
    "Anatomical subsite of tumour", "Year of diagnosis 1985-94", "Age in 4 categories", "Date of diagnosis",
    "Date of exit")
  private val COLON_COLUMN_TYPES : Seq[Class[Number]] = Seq(
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number],
    classOf[Number])
  private val COLON_COLUMN_LENGTHS = Seq(8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8)
  private val COLON_SAS_FILE_PROPERTIES = new SasFileProperties(
    false, null, 1, "US-ASCII", null, "colon","DATA",
    Instant.ofEpochMilli(854409600000L).atOffset(ZoneOffset.UTC).toLocalDateTime,
    Instant.ofEpochMilli(854409600000L).atOffset(ZoneOffset.UTC).toLocalDateTime,
    "7.00.00B", "WIN_95", "WIN", "",
    1024, 262144, 7, 15564,
    104, 2493, 13)
  private val COMPARE_ROWS_COUNT = 300
  private val COLON_SAS7BDAT_URL = "http://biostat3.net/download/sas/colon.sas7bdat"
  private val fileName = DEFAULT_FILE_NAME

  "The SAS dataset reader" should "read column names properly" in {

  }

  "The SAS dataset reader" should "read the metadata info properly" in {

  }

  "The SAS dataset reader" should "read in 15564 rows" in {

  }

}
