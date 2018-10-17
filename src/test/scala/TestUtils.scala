package com.kaszub.parso

import java.io.File

import scala.io.{BufferedSource, Source}
import com.typesafe.scalalogging.Logger

object TestUtils {
  private val logger = Logger[this.type]

  def getResourceAsStream(fileName: String) = Source.fromResource(fileName)

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
