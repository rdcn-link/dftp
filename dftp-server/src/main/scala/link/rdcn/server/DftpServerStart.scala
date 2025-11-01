package link.rdcn.server

import link.rdcn.server.module.{AuthModule, BaseDftpModule, DirectoryDataSourceModule, RequireAuthenticatorEvent}
import link.rdcn.user.{AuthenticationRequest, AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}

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
      props.getProperty("dftp.host.port").toInt, Some(dftpHome))
    val server = new DftpServer(dftpServerConfig)
    server.addModule(new BaseDftpModule)
      .addModule(new AuthModule(authenticationService))
      .addModule(new DirectoryDataSourceModule)
    server.startBlocking()
  }

  private val authenticationService = new AuthenticationService {
    override def accepts(request: AuthenticationRequest): Boolean = true

    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }
}
