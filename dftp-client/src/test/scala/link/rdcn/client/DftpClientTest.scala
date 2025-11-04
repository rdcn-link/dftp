package link.rdcn.server

import link.rdcn.client.DftpClient
import link.rdcn.server.module.{BaseDftpModule, DirectoryDataSourceModule, UserPasswordAuthModule}
import link.rdcn.user.{UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 19:13
 * @Modified By:
 */
object DftpServerTest{

  var server: DftpServer = _

  private val userPasswordAuthService = new UserPasswordAuthService {
    override def authenticate(credentials: UsernamePassword): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def accepts(credentials: UsernamePassword): Boolean = true
  }

  @BeforeAll
  def startServer(): Unit = {
    val directoryDataSourceModule = new DirectoryDataSourceModule
    val modules = Array(directoryDataSourceModule, new BaseDftpModule,
      new UserPasswordAuthModule(userPasswordAuthService))
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3102, Some("data")), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    server.close()
  }

}

class DftpServerTest {

  @Test
  def getStreamTest(): Unit = {
    val client = DftpClient.connect("dftp://0.0.0.0:3102")
    val df = client.get("dftp://0.0.0.0:3102/")
    df.foreach(println)
  }

}
