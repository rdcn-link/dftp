package link.rdcn.dacp

import link.rdcn.dacp.catalog.{DacpCatalogModule, DirectoryCatalogModule}
import link.rdcn.dacp.cook.DacpCookModule
import link.rdcn.server.{DftpServer, DftpServerConfig}
import link.rdcn.server.module.{BaseDftpModule, FileDirectoryDataSourceModule, UserPasswordAuthModule}
import link.rdcn.user.{UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.file.Paths
import java.util.Properties

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/6 16:57
 * @Modified By:
 */
object DacpServerStart {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) sys.error("need set Dacp Home")
    val dftpHome = args(0)

    val configXmlFile = Paths.get(dftpHome, "conf", "dacp.xml").toFile
    DftpServer.start(configXmlFile)
  }

}
