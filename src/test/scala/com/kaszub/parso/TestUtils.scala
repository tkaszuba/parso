package com.kaszub.parso

import java.io.File

import com.typesafe.scalalogging.Logger

import scala.io.Source

object TestUtils {
  private val logger = Logger[this.type]

  def getResourceAsStream(fileName: String) = Source.fromResource(fileName)

  def getConst[T](obj : Any, fieldName : String): T = {
    val field = obj.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(obj).asInstanceOf[T]
  }

  //def getStringConst(obj: Any, fieldName:String): String = getConst(obj,fieldName).toString

  def getSeqConst(obj: Any, fieldName:String): String = getConst(obj,fieldName).toString

  def getSas7bdatFilesList(fileOrFolderName: String): Seq[File] = {

    val fileOrFolder = new File(fileOrFolderName)

    if (fileOrFolder.isFile)
      Seq(fileOrFolder)
    else if (fileOrFolder.isDirectory)
      fileOrFolder.listFiles
        .filter(file => file.isFile && file.getName.toLowerCase.endsWith(".sas7bdat"))
        .toSeq
    else
      logger.error(s"Wrong file name ${fileOrFolderName}")

    throw new IllegalArgumentException(s"The passed argument is invalid ${fileOrFolderName}")

  }
}
