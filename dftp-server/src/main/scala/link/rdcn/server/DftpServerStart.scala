package link.rdcn.server

import link.rdcn.server.module.{AuthModule, BaseDftpModule, DirectoryDataSourceModule, RequireAuthenticatorEvent}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

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
    val props = loadProperties(dftpHome + File.separator + "conf" + File.separator + "dftp.conf")
    val dftpServerConfig = DftpServerConfig(props.getProperty("dftp.host.position"),
      props.getProperty("dftp.host.port").toInt, Some(dftpHome),Some(props.getProperty("dftp.datasource")))
    val directoryDataSourceModule = new DirectoryDataSourceModule
    directoryDataSourceModule.setRootDirectory(new File(dftpServerConfig.dftpDataSource.getOrElse("")))
    val server = new DftpServer(dftpServerConfig) {
      modules.addModule(new BaseDftpModule)
        .addModule(new AuthModule(authenticationService))
        .addModule(directoryDataSourceModule)
    }
    server.startBlocking()
  }

  private val authenticationService = new AuthenticationService {
    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def accepts(credentials: Credentials): Boolean = true
  }

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }
}
