package link.rdcn.dacp.utils

import java.nio.file.{Files, Paths}

object FileUtils {

  def getTempDirectory(baseDir: String, id: String): String = {
    val tmpDir = Paths.get(baseDir, s"container_${id}").toAbsolutePath
    Files.createDirectories(tmpDir)
    tmpDir.toString
  }

}
