package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.DataFrame

import java.io.File
import java.nio.file.Files

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/29 14:57
 * @Modified By:
 */
abstract class FilePipe(file: File) {
  def create(): Unit = {
    if (!file.exists()) {
      Runtime.getRuntime.exec(Array("mkfifo", file.getAbsolutePath)).waitFor()
    }
  }

  def delete(): Unit = {
    Files.deleteIfExists(file.toPath)
  }

  def path: String = file.getAbsolutePath

  def dataFrame(): DataFrame
}
