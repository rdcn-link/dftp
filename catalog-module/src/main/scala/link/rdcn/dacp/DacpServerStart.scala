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
    val dacpHome = args(0)
    val props = loadProperties(dacpHome + File.separator + "conf" + File.separator + "dacp.conf")
    val dftpServerConfig = DftpServerConfig(props.getProperty("dacp.host.position"),
      props.getProperty("dacp.host.port").toInt, Some(dacpHome))

    val dataPathFile = Paths.get(dacpHome,"data").toFile
    val directoryDataSourceModule = new FileDirectoryDataSourceModule
    directoryDataSourceModule.setRootDirectory(dataPathFile)
    val directoryCatalogModule = new DirectoryCatalogModule
    directoryCatalogModule.setRootDirectory(dataPathFile)

    val server = new DftpServer(dftpServerConfig) {
      modules.addModule(new BaseDftpModule)
        .addModule(new UserPasswordAuthModule(userPasswordAuthService))
        .addModule(new FileDirectoryDataSourceModule)
        .addModule(new DacpCookModule)
        .addModule(new DacpCatalogModule)
        .addModule(directoryCatalogModule)
    }
    server.startBlocking()
  }

  private val userPasswordAuthService = new UserPasswordAuthService {
    override def authenticate(credentials: UsernamePassword): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def accepts(credentials: UsernamePassword): Boolean = true
  }

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }
}
