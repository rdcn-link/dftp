package link.rdcn.server

import link.rdcn.server.module.{BaseDftpModule, DirectoryDataSourceModule}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 16:12
 * @Modified By:
 */

class AuthModule extends DftpModule{

  private val authenticationService = new AuthenticationService {
    override def accepts(credentials: Credentials): Boolean = true

    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(authenticationService)

  override def destroy(): Unit = {}
}

object DftpServerStart {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) sys.error("need set Dftp Home")
    val dftpHome = args(0)
    val props = loadProperties(dftpHome + File.separator + "conf" + File.separator + "dftp.conf")
    val dftpServerConfig = DftpServerConfig(props.getProperty("dftp.host.position"),
      props.getProperty("dftp.host.port").toInt, Some(dftpHome))
    val server = new DftpServer(dftpServerConfig)
    server.addModule(new BaseDftpModule)
      .addModule(new AuthModule)
      .addModule(new DirectoryDataSourceModule)
    server.startBlocking()
  }
  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }
}
