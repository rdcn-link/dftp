package link.rdcn

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
import link.rdcn.DacpModuleTestBase._
import link.rdcn.catalog.DacpCatalogModule
import link.rdcn.client.DftpClient
import link.rdcn.server.module.{BaseDftpModule, DirectoryDataSourceModule, RequireAuthenticatorEvent}
import link.rdcn.server._
import link.rdcn.struct._
import link.rdcn.user.{AuthenticationRequest, AuthenticationService, UserPrincipalWithCredentials, UsernamePassword}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.File
import java.nio.file.Paths

trait DacpModuleTestProvider {

}

object DacpModuleTestProvider {

  private var server: Option[DftpServer] = None
  val host = "0.0.0.0"
  val port = 3101
  val baseUrl = s"dftp://$host:$port"

  val resourceUrl = getClass.getProtectionDomain.getCodeSource.getLocation
  val testClassesDir = new File(resourceUrl.toURI)
  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = Paths.get(baseDir, "bin").toString
  val csvDir = Paths.get(baseDir, "csv").toString

  //必须在DfInfos前执行一次
  DacpModuleTestDataGenerator.generateTestData(binDir, csvDir, baseDir)


  var dc: DftpClient = _
  var expectedHostInfo: Map[String, String] = _

  @BeforeAll
  def startServer(): Unit = {
    DacpModuleTestDataGenerator.generateTestData(binDir, csvDir, baseDir)
    getServer
    connectClient
  }

  @AfterAll
  def stop(): Unit = {
    stopServer()
    BlobRegistry.cleanUp()
    DacpModuleTestDataGenerator.cleanupTestData(baseDir)
  }

  def getServer: DftpServer = synchronized {
    if (server.isEmpty) {
      val directoryDataSourceModule = new DirectoryDataSourceModule
      directoryDataSourceModule.setRootDirectory(new File(baseDir))
      val modules = Array(directoryDataSourceModule, new BaseDftpModule, new MockAuthModule, new DacpCatalogModule)
      val s = DftpServer.start(DftpServerConfig("0.0.0.0", 3101, Some("data")), modules)
      server = Some(s)

    }
    server.get
  }

  def connectClient: Unit = synchronized {
    dc = DftpClient.connect("dftp://0.0.0.0:3101", UsernamePassword(adminUsername, adminPassword))
  }

  def stopServer(): Unit = synchronized {
    server.foreach(_.close())
    server = None
  }

}

class MockAuthModule extends DftpModule{

  private val authenticationService = new AuthenticationService {
    override def accepts(request: AuthenticationRequest): Boolean = true

    override def authenticate(credentials: user.Credentials): user.UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = event.isInstanceOf[RequireAuthenticatorEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireAuthenticatorEvent => r.add(authenticationService)
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}
}





