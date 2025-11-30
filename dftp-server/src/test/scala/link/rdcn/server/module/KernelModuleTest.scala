/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 18:28
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationMethod, Credentials, UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class KernelModuleTest {

  // --- 模拟的“世界” (Anchor, EventHub, Context) ---

  /**
   * 模拟 EventHub，用于捕获所有被触发的事件
   */
  class MockEventHub extends EventHub {
    val eventsFired = new ArrayBuffer[CrossModuleEvent]()
    override def fireEvent(event: CrossModuleEvent): Unit = {
      eventsFired.append(event)
    }
  }

  /**
   * 模拟 Anchor，用于捕获 KernelModule hook 的 EventSource
   */
  class MockAnchor extends Anchor {
    var hookedEventSource: EventSource = null

    // 模拟 Anchor 的 hook(EventSource) 方法
    override def hook(service: EventSource): Unit = {
      this.hookedEventSource = service
    }

    // (为编译而添加的 DftpModule.scala 中定义的其他 hook 方法的存根)
    override def hook(service: EventHandler): Unit = {}
  }

  /**
   * 模拟一个 ServerContext
   */
  class MockServerContext extends ServerContext {
    override def getHost(): String = "mock-host"
    override def getPort(): Int = 1234
    override def getProtocolScheme(): String = "dftp"
    override def getDftpHome(): Option[String] = None
  }

  // --- 模拟的服务 (用于注入 Holder) ---

  class MockActionHandler extends ActionMethod {
    var doActionCalled = false
    var requestCalledWith: DftpActionRequest = null
    override def accepts(request: DftpActionRequest): Boolean = true
    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      doActionCalled = true
      requestCalledWith = request
    }
  }

  class MockGetStreamHandler extends GetStreamMethod {
    var doGetStreamCalled = false
    override def accepts(request: DftpGetStreamRequest): Boolean = true
    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
      doGetStreamCalled = true
    }
  }

  class MockPutStreamHandler extends PutStreamMethod {
    var doPutStreamCalled = false
    override def accepts(request: DftpPutStreamRequest): Boolean = true
    override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
      doPutStreamCalled = true
    }
  }

  class MockGetStreamRequestParser extends ParseRequestMethod {
    var parseCalled = false
    val requestToReturn: DftpGetStreamRequest = new MockDftpGetStreamRequest() // 模拟返回
    override def accepts(token: Array[Byte]): Boolean = true
    override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
      parseCalled = true
      requestToReturn
    }
  }

  class MockAccessLogger extends AccessLogger {
    var doLogCalled = false
    override def accepts(request: DftpRequest): Boolean = true
    override def doLog(request: DftpRequest, response: DftpResponse): Unit = {
      doLogCalled = true
    }
  }

  class MockAuthenticationService extends UserPasswordAuthService {
    var authenticateCalled = false
    val userToReturn: UserPrincipal = MockUserPrincipal("MockAuthUser")

    override def accepts(credentials: Credentials): Boolean = true

    override def authenticate(credentials: Credentials): UserPrincipal = {
      authenticateCalled = true
      userToReturn
    }
  }

  // --- 模拟的请求/响应/凭证/用户 (用于传递参数) ---

  object MockCredentials extends UsernamePassword("MockUser", "MockPass")
  case class MockUserPrincipal(name: String) extends UserPrincipal {
    def getName: String = name
  }

  // 模拟的 Response，用于捕获 sendError
  class MockDftpActionResponse extends DftpActionResponse {
    var errorSent = false
    var errorCode = 0
    var message = ""
    override def sendError(errorCode: Int, message: String): Unit = {
      errorSent = true
      this.errorCode = errorCode
      this.message = message
    }
    override def sendData(data: Array[Byte]): Unit = {}
  }
  class MockDftpGetStreamResponse extends DftpGetStreamResponse {
    var errorSent = false
    var errorCode = 0
    var message = ""
    override def sendError(errorCode: Int, message: String): Unit = {
      errorSent = true
      this.errorCode = errorCode
      this.message = message
    }
    override def sendDataFrame(dataFrame: DataFrame): Unit = {}
  }
  class MockDftpPutStreamResponse extends DftpPutStreamResponse {
    var errorSent = false
    var errorCode = 0
    var message = ""
    override def sendError(errorCode: Int, message: String): Unit = {
      errorSent = true
      this.errorCode = errorCode
      this.message = message
    }
    override def sendData(data: Array[Byte]): Unit = {}
  }

  class MockDftpActionRequest(action: String = "test") extends DftpActionRequest {
    override def getActionName(): String = action
    override def getParameter(): Array[Byte] = Array.empty
    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockDftpGetStreamRequest extends DftpGetStreamRequest {
    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockDftpPutStreamRequest extends DftpPutStreamRequest {
    override def getDataFrame(): DataFrame = null
    override def getUserPrincipal(): UserPrincipal = null
  }


  // --- 测试状态变量 ---

  private var kernelModule: KernelModule = _
  private var mockEventHub: MockEventHub = _

  // 从 EventHub 捕获的 Holders
  private var authHolder: Workers[AuthenticationMethod] = _
  private var parseHolder: Workers[ParseRequestMethod] = _
  private var getHolder: FilteredGetStreamMethods = _
  private var actionHolder: Workers[ActionMethod] = _
  private var putHolder: Workers[PutStreamMethod] = _

  @BeforeEach
  def setUp(): Unit = {
    // 1. 创建所有实例
    kernelModule = new KernelModule()
    val mockAnchor = new MockAnchor()
    mockEventHub = new MockEventHub()
    val mockContext = new MockServerContext()

    // 2. 执行 init()
    kernelModule.init(mockAnchor, mockContext)

    // 3. 验证 EventSource 被 hook
    assertNotNull(mockAnchor.hookedEventSource, "KernelModule.init 未能 hook EventSource")

    // 4. 执行 EventSource.init() 来触发所有事件
    mockAnchor.hookedEventSource.init(mockEventHub)

    // 5. 提取所有 Holders 以供测试使用
    authHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectAuthenticationMethodEvent]).get
      .asInstanceOf[CollectAuthenticationMethodEvent].collector

    parseHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectParseRequestMethodEvent]).get
      .asInstanceOf[CollectParseRequestMethodEvent].collector

    getHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectGetStreamMethodEvent]).get
      .asInstanceOf[CollectGetStreamMethodEvent].collector

    actionHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectActionMethodEvent]).get
      .asInstanceOf[CollectActionMethodEvent].collector

    putHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectPutStreamMethodEvent]).get
      .asInstanceOf[CollectPutStreamMethodEvent].collector
  }

  /**
   * 测试 KernelModule.init 是否触发了所有 6 个必需的事件
   */
  @Test
  def testInit_FiresAllEvents(): Unit = {
    assertEquals(6, mockEventHub.eventsFired.length, "init() 应触发 6 个事件")

    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectAuthenticationMethodEvent]), "RequireAuthenticatorEvent 未被触发")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectParseRequestMethodEvent]), "RequireGetStreamRequestParserEvent 未被触发")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectActionMethodEvent]), "RequireActionHandlerEvent 未被触发")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectPutStreamMethodEvent]), "RequirePutStreamHandlerEvent 未被触发")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectGetStreamMethodEvent]), "RequireGetStreamHandlerEvent 未被触发")
  }

  // --- 测试 API 委托 (Happy Path & Null Path) ---

  @Test
  def testDoAction_WithHandler(): Unit = {
    val mockHandler = new MockActionHandler()
    val mockRequest = new MockDftpActionRequest("test-action")
    val mockResponse = new MockDftpActionResponse()

    actionHolder.add(mockHandler) // 注入
    kernelModule.doAction(mockRequest, mockResponse)

    assertTrue(mockHandler.doActionCalled, "注入的 ActionHandler.doAction 应被调用")
    assertEquals(mockRequest, mockHandler.requestCalledWith, "ActionHandler.doAction 接收到的 request 不匹配")
    assertFalse(mockResponse.errorSent, "当 Handler 存在时，不应调用 onNull (sendError)")
  }

  @Test
  def testDoAction_NoHandler(): Unit = {
    val mockRequest = new MockDftpActionRequest("test-action")
    val mockResponse = new MockDftpActionResponse()

    // 不注入 Handler
    kernelModule.doAction(mockRequest, mockResponse)

    assertTrue(mockResponse.errorSent, "当 Handler 为 null 时，应调用 onNull (sendError)")
    assertEquals(404, mockResponse.errorCode, "onNull 应发送 404 错误")
    assertTrue(mockResponse.message.contains("test-action"), "onNull 的错误消息应包含 action 名称")
  }

  @Test
  def testGetStream_WithHandler(): Unit = {
    val mockHandler = new MockGetStreamHandler()
    val mockRequest = new MockDftpGetStreamRequest()
    val mockResponse = new MockDftpGetStreamResponse()

    getHolder.addMethod(mockHandler) // 注入
    kernelModule.getStream(mockRequest, mockResponse)

    assertTrue(mockHandler.doGetStreamCalled, "注入的 GetStreamHandler.doGetStream 应被调用")
    assertFalse(mockResponse.errorSent, "当 Handler 存在时，不应调用 onNull (sendError)")
  }

  @Test
  def testGetStream_NoHandler(): Unit = {
    val mockRequest = new MockDftpGetStreamRequest()
    val mockResponse = new MockDftpGetStreamResponse()

    // 不注入 Handler
    kernelModule.getStream(mockRequest, mockResponse)

    assertTrue(mockResponse.errorSent, "当 Handler 为 null 时，应调用 onNull (sendError)")
    assertEquals(404, mockResponse.errorCode, "onNull 应发送 404 错误")
  }

  @Test
  def testPutStream_WithHandler(): Unit = {
    val mockHandler = new MockPutStreamHandler()
    val mockRequest = new MockDftpPutStreamRequest()
    val mockResponse = new MockDftpPutStreamResponse()

    putHolder.add(mockHandler) // 注入
    kernelModule.putStream(mockRequest, mockResponse)

    assertTrue(mockHandler.doPutStreamCalled, "注入的 PutStreamHandler.doPutStream 应被调用")
    assertFalse(mockResponse.errorSent, "当 Handler 存在时，不应调用 onNull (sendError)")
  }

  @Test
  def testPutStream_NoHandler(): Unit = {
    val mockRequest = new MockDftpPutStreamRequest()
    val mockResponse = new MockDftpPutStreamResponse()

    // 不注入 Handler
    kernelModule.putStream(mockRequest, mockResponse)

    assertTrue(mockResponse.errorSent, "当 Handler 为 null 时，应调用 onNull (sendError)")
    assertEquals(500, mockResponse.errorCode, "onNull 应发送 500 错误")
  }

  @Test
  def testParseGetStreamRequest_WithHandler(): Unit = {
    val mockParser = new MockGetStreamRequestParser()
    val mockToken = Array[Byte](1, 2)
    val mockPrincipal = new MockUserPrincipal("test")

    parseHolder.add(mockParser) // 注入
    val result = kernelModule.parseGetStreamRequest(mockToken, mockPrincipal)

    assertTrue(mockParser.parseCalled, "注入的 GetStreamRequestParser.parse 应被调用")
    assertEquals(mockParser.requestToReturn, result, "返回的 DftpGetStreamRequest 不匹配")
  }

  @Test
  def testParseGetStreamRequest_NoHandler(): Unit = {
    val mockToken = Array[Byte](1, 2)
    val mockPrincipal = new MockUserPrincipal("test")

    // 不注入 Handler
    val ex = assertThrows(classOf[Exception], () => {
      kernelModule.parseGetStreamRequest(mockToken, mockPrincipal)
      ()
    }, "当 Parser 为 null 时应抛出异常")

    assertTrue(ex.getMessage.contains("GetStreamRequestParser"), "异常消息应指明 Parser 未设置")
  }

  @Test
  def testAuthenticate_WithHandler(): Unit = {
    val mockAuth = new MockAuthenticationService()

    authHolder.add(mockAuth) // 注入
    val user = kernelModule.authenticate(MockCredentials)

    //FIXME: Redundant check of Workers.work(), which is already completed by WorkerTest!!! check all similar tests to clean redundant code
    assertTrue(mockAuth.authenticateCalled, "注入的 AuthenticationService.authenticate 应被调用")
    assertEquals(mockAuth.userToReturn, user, "返回的 UserPrincipal 不匹配")
  }

  @Test
  def testAuthenticate_NoHandler(): Unit = {
    // 不注入 Handler
    val user = kernelModule.authenticate(MockCredentials)

    // 验证 onNull (FIXME) 的默认行为
    assertTrue(user.isInstanceOf[UserPrincipalWithCredentials], "onNull 应返回 UserPrincipalWithCredentials")
    assertEquals(MockCredentials, user.asInstanceOf[UserPrincipalWithCredentials].credentials, "onNull 返回的凭证不匹配")
  }
}