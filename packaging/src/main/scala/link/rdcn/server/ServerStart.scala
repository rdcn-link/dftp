package link.rdcn.server

import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 16:12
 * @Modified By:
 */

object ServerStart {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) sys.error("need set Dftp Home")
    val serverHome = args(0)

    val confDir = Paths.get(serverHome, "conf").toFile

    if (confDir.exists() && confDir.isDirectory) {
      val xmlFiles = confDir.listFiles().filter(f => f.isFile && f.getName.endsWith(".xml"))
      if (xmlFiles.length == 1) {
        val configXmlFile = xmlFiles.head
        DftpServer.start(configXmlFile)
      } else {
        throw new RuntimeException(s"Unexpected number of XML files found in the conf directory: ${xmlFiles.length}")
      }
    } else {
      throw new RuntimeException(s"Conf directory does not exist: $confDir")
    }
  }
}
