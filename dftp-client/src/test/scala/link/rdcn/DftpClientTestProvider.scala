package link.rdcn

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
import link.rdcn.DftpClientTestBase._
import link.rdcn.client.DftpClient
import link.rdcn.server._
import link.rdcn.server.module._
import link.rdcn.struct._
import link.rdcn.user.{Credentials, UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.File
import java.nio.file.Paths

trait DftpClientTestProvider {

}

object DftpClientTestProvider {

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
  DftpClientTestDataGenerator.generateTestData(binDir, csvDir, baseDir)

  private val userPasswordAuthService = new UserPasswordAuthService {
    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def accepts(credentials: Credentials): Boolean = true
  }


  var dc: DftpClient = _
  var expectedHostInfo: Map[String, String] = _

  @BeforeAll
  def startServer(): Unit = {
    DftpClientTestDataGenerator.generateTestData(binDir, csvDir, baseDir)
    getServer
    connectClient
  }

  @AfterAll
  def stop(): Unit = {
    stopServer()
    BlobRegistry.cleanUp()
//    DftpClientTestDataGenerator.cleanupTestData(baseDir)
  }

  def getServer: DftpServer = synchronized {
    val userPasswordAuthService = new UserPasswordAuthService {
      override def authenticate(credentials: Credentials): UserPrincipal =
        UserPrincipalWithCredentials(credentials)

      override def accepts(credentials: Credentials): Boolean = true
    }
    if (server.isEmpty) {
//      val directoryDataSourceModule = new FileDirectoryDataSourceModule
//      directoryDataSourceModule.setRootDirectory(new File(baseDir))
      val modules = Array(new BaseDftpModule,
        new UserPasswordAuthModule(userPasswordAuthService))
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

class PutModule extends DftpModule {
  private val putStreamHolder = new Workers[PutStreamMethod]
  private var serverContext: ServerContext = _
  private val putStreamHandler = new PutStreamMethod {
    override def accepts(request: DftpPutStreamRequest): Boolean = true

    override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
      response.sendData("success".getBytes)
    }
  }
  private val eventHandler = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean = true

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectPutStreamMethodEvent => r.collector.add(putStreamHandler)
          case _ =>
        }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext
    anchor.hook(eventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(new CollectPutStreamMethodEvent(putStreamHolder))
    })
  }

  override def destroy(): Unit = {
  }
}

class ActionModule extends DftpModule {
  private val actionHolder = new Workers[ActionMethod]
  private var serverContext: ServerContext = _
  private val actionHandler = new ActionMethod {

    override def accepts(request: DftpActionRequest): Boolean = true

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = response.sendJsonString("")
  }
  private val eventHandler = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean = true

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case r: CollectActionMethodEvent => r.collector.add(actionHandler)
        case _ =>
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext
    anchor.hook(eventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(new CollectActionMethodEvent(actionHolder))
    })
  }

  override def destroy(): Unit = {
  }
}