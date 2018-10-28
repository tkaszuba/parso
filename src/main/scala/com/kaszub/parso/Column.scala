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
case class Column (
                    id : Option[Int] = None,
                    name : Option[String] = None,
                    label: ColumnLabel,
                    format : ColumnFormat,
                    _type : Option[Class[_]] = None,
                    length: Option[Int] = None) {

  override def toString: String = {
    s"id: %s name: %s label: %s format: %s _type: %s length: %s".format(
      id.getOrElse(""),
      name.getOrElse(""),
      label.alias match {case Left(v) => v case _ => ""},
      format.toString,
      _type match {case Some(v) => v.getName case _ => ""},
      length.getOrElse(""))
  }
}
case class ColumnAttributes(offset: Long, length: Int, _type: Class[_])
