package com.kaszub.parso

/**
  * Class used to store SAS Column Metadata.
  *
  * @param id     the column id
  * @param name   the column name.
  * @param label  the column label.
  * @param format the column format
  * @param _type  the class of data stored in cells of rows related to the column, can be Number.class or
  *               String.class.
  * @param length the column length
  */
case class Column (id : Int, name : String, label: String, format : ColumnFormat, _type : Class[T], length: Int)
