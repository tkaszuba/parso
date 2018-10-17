package com.kaszub.parso

/**
  * Class used to store SAS Column Metadata.
  *
  * @param name      - column format name.
  * @param width     - column format width.
  * @param precision - column format precision.
  */
case class ColumnFormat(name: String, width: Int, precision: Int) {

  /**
    * Returns true if there is no information about column format, otherwise false.
    *
    * @return true if column name is empty and width and precision are 0, otherwise false.
    */
  def isEmpty: Boolean = name.isEmpty && (width eq 0) && (precision eq 0)

  /**
    * The function to ColumnFormat class string representation.
    *
    * @return string representation of the column format.
    */
  override def toString: String = {
    def print(num: Int) : String = if (num != 0) s"${num}" else ""

    if (isEmpty)
      ""
    else
      s"${name}${print(width)}.${print(precision)}"
  }

}
