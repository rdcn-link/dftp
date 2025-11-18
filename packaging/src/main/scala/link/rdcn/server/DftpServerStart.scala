package link.rdcn.server

import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 16:12
 * @Modified By:
 */

object DftpServerStart {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) sys.error("need set Dftp Home")
    val dftpHome = args(0)

    val configXmlFile = Paths.get(dftpHome, "conf", "dftp.xml").toFile
    DftpServer.start(configXmlFile)
  }
}
