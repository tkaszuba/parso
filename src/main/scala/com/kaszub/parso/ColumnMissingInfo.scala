package com.kaszub.parso

import com.kaszub.parso.MissingInfoType._

/**
  * A class to store info about column if it's on page type "amd".
  *
  * @param columnId the column id.
  * @param textSubheaderIndex the text subheader index.
  * @param offset the missing information offset.
  * @param length the missing information length.
  * @param missingInfoType the missing information type.
  */
case class ColumnMissingInfo(columnId : Int, textSubheaderIndex : Int, offset : Int, length : Int,
                             missingInfoType : MissingInfoType)
