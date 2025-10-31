package link.rdcn.server

import link.rdcn.client.DftpClient
import link.rdcn.server.module.{BaseDftpModule, DirectoryDataSourceModule, RequiresAuthenticator}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType, ValueType}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}
import link.rdcn.util.{CodecUtils, DataUtils}
import org.json.JSONObject
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.File

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 19:13
 * @Modified By:
 */


class AuthModule extends DftpModule{

  private val authenticationService = new AuthenticationService {
    override def accepts(credentials: Credentials): Boolean = true

    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = event.isInstanceOf[RequiresAuthenticator]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequiresAuthenticator => r.add(authenticationService)
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}
}


object DftpServerTest{

  var server: DftpServer = _

  @BeforeAll
  def startServer(): Unit = {
    val directoryDataSourceModule = new DirectoryDataSourceModule
    directoryDataSourceModule.setRootDirectory(new File("/Users/renhao/IdeaProjects/dftp_1031/packaging/src/main/distribution/data"))
    val modules = Array(directoryDataSourceModule, new BaseDftpModule, new AuthModule)
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
    val df = client.get("dftp://0.0.0.0:3102/nodes")
    df.foreach(println)
  }

}
