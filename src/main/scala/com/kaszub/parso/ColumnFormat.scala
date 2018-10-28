package com.kaszub.parso

/**
  * Class used to store SAS Column Metadata.
  *
  * @param name      column format name.
  * @param width     column format width.
  * @param precision column format precision.
  */
//TODO: Consider renaming width to precision and precision to scale as it's confusing right now
case class ColumnFormat(name: Either[String, ColumnMissingInfo], width: Int, precision: Int) {

  /**
    * Returns true if there is no information about column format, otherwise false.
    *
    * @return true if column name is empty and width and precision are 0, otherwise false.
    */
  def isEmpty: Boolean = {
    name match {
      case Left(v) => v.isEmpty && (width == 0) && (precision == 0)
      case Right(_) => (width == 0) && (precision == 0)
      case _ => false
    }
  }

  /**
    * The function to ColumnFormat class string representation.
    *
    * @return string representation of the column format.
    */
  override def toString: String = {
    def print(num: Int): String = if (num != 0) s"${num}" else ""

    if (isEmpty)
      ""
    else {
      val e = name match {case Left(v) => v.toString case _ => ""}
      s"${e}.${print(width)}.${print(precision)}"
    }
  }
}
