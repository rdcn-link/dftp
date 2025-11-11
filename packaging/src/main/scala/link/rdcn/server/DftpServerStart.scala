package link.rdcn.server

import link.rdcn.server.module.{BaseDftpModule, FileDirectoryDataSourceModule, UserPasswordAuthModule}
import link.rdcn.user.{UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.file.Paths
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
    val props = loadProperties(dftpHome + File.separator + "conf" + File.separator + "dacp.conf")
    val dftpServerConfig = DftpServerConfig(props.getProperty("dftp.host.position"),
      props.getProperty("dftp.host.port").toInt, Some(dftpHome))

    val dataPathFile = Paths.get(dftpHome,"data").toFile
    val directoryDataSourceModule = new FileDirectoryDataSourceModule
    directoryDataSourceModule.setRootDirectory(dataPathFile)

    val server = new DftpServer(dftpServerConfig) {
      modules.addModule(new BaseDftpModule)
        .addModule(new UserPasswordAuthModule(userPasswordAuthService))
        .addModule(new FileDirectoryDataSourceModule)
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
