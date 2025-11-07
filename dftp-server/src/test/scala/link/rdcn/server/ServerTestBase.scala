/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/7 18:09
 * @Modified By:
 */
package link.rdcn.server

import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.server.module.DataFrameProviderService
import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}
import org.json.JSONObject

import scala.collection.mutable.ArrayBuffer


/**
 * 模拟一个 Anchor，用于捕获被 hook 的 EventHandler
 */
class MockAnchor extends Anchor {
  var hookedHandler: EventHandler = null
  var hookedEventSource: EventSource = null
  val hookedEventHandlers = new ArrayBuffer[EventHandler]()

  override def hook(service: EventHandler): Unit = {
    this.hookedHandler = service
    this.hookedEventHandlers.append(service)
  }

  override def hook(service: EventSource): Unit = {
    this.hookedEventSource = service
  }
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

/**
 * 模拟一个不相关的事件
 */
class OtherMockEvent extends CrossModuleEvent

/**
 * 模拟一个 UserPrincipal
 */
case object MockUser extends UserPrincipal {
  def getName: String = "MockUser"
}

/**
 * 模拟一个 Credentials
 */
case object MockCredentials extends Credentials

/**
 * 模拟 AuthenticationService，用于跟踪调用
 */
class MockAuthenticationService(name: String) extends AuthenticationService {
  var acceptsCreds: Boolean = false
  var userToReturn: UserPrincipal = MockUser
  var acceptsCalled: Boolean = false
  var authenticateCalled: Boolean = false
  var credsChecked: Credentials = null

  override def accepts(credentials: Credentials): Boolean = {
    acceptsCalled = true
    acceptsCreds
  }
  override def authenticate(credentials: Credentials): UserPrincipal = {
    authenticateCalled = true
    credsChecked = credentials
    userToReturn
  }

  override def toString: String = s"MockAuthenticationService($name)"
}
/**
 * 模拟一个 Anchor，用于捕获被 hook 的 EventHandler
 * (复用自 AuthModuleTest)
 */
class MockFlowForProvider extends Anchor {
  var hookedHandler: EventHandler = null

  override def hook(service: EventHandler): Unit = {
    this.hookedHandler = service
  }

  override def hook(service: EventSource): Unit = ???
}

/**
 * 模拟一个 ServerContext
 * (复用自 AuthModuleTest)
 */
class MockServerContextForProvider extends ServerContext {
  override def getHost(): String = "mock-host"
  override def getPort(): Int = 1234
  override def getProtocolScheme(): String = "dftp"
  override def getDftpHome(): Option[String] = None
}

/**
 * 模拟一个不相关的事件
 * (复用自 AuthModuleTest)
 */
class OtherMockEventForProvider extends CrossModuleEvent

/**
 * 模拟一个 UserPrincipal
 * (复用自 AuthModuleTest)
 */
case object MockUserForProvider extends UserPrincipal {
  def getName: String = "MockUserForProvider"
}

/**
 * 模拟 DataFrameProviderService
 */
class MockDataFrameProviderServiceForBase(dfToReturn: Option[DataFrame], exceptionToThrow: Option[Exception] = None) extends DataFrameProviderService {
  var getDataFrameCalled = false
  var urlChecked: String = null
  override def accepts(dataFrameUrl: String): Boolean = true // 假设接受
  override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                           (implicit ctx: ServerContext): DataFrame = {
    getDataFrameCalled = true
    urlChecked = dataFrameUrl
    if (exceptionToThrow.isDefined) throw exceptionToThrow.get
    dfToReturn.getOrElse(throw new DataFrameNotFoundException(s"Mock DF $dataFrameUrl not found"))
  }
}

/**
 * 模拟 DataFrameProviderService，用于跟踪调用
 */
class MockDataFrameProviderService(name: String) extends DataFrameProviderService {
  var acceptsUrl: Boolean = false
  var dfToReturn: DataFrame = DefaultDataFrame(StructType.empty, Iterator.empty)
  var acceptsCalled: Boolean = false
  var getDataFrameCalled: Boolean = false
  var urlChecked: String = null

  override def accepts(dataFrameUrl: String): Boolean = {
    acceptsCalled = true
    acceptsUrl
  }

  override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                           (implicit ctx: ServerContext): DataFrame = {
    getDataFrameCalled = true
    urlChecked = dataFrameUrl
    dfToReturn
  }

  override def toString: String = s"MockDataFrameProviderService($name)"
}

/**
 * 模拟一个 Anchor，用于捕获被 hook 的 EventHandler
 */
class MockAnchorForDirectory extends Anchor {
  var hookedHandler: EventHandler = null
  var hookedEventSource: EventSource = null
  val hookedEventHandlers = new ArrayBuffer[EventHandler]()

  override def hook(service: EventHandler): Unit = {
    this.hookedHandler = service
    this.hookedEventHandlers.append(service)
  }

  override def hook(service: EventSource): Unit = {
    this.hookedEventSource = service
  }
}

/**
 * 模拟一个 ServerContext
 */
class MockServerContextForDirectory extends ServerContext {
  override def getHost(): String = "mock-host"
  override def getPort(): Int = 1234
  override def getProtocolScheme(): String = "dftp"
  // baseUrl 必须以 / 结尾
  override def baseUrl: String = "dftp://mock-host:1234/"
  override def getDftpHome(): Option[String] = None
}

/**
 * 模拟一个不相关的事件
 */
class OtherMockEventForDirectory extends CrossModuleEvent

/**
 * 模拟一个 UserPrincipal
 */
case object MockUserForDirectory extends UserPrincipal {
  def getName: String = "MockUserForDirectory"
}

/**
 * 模拟 DataFrameProviderService，用于测试链式调用 (old service)
 */
class MockDataFrameProviderServiceForDirectory(name: String) extends DataFrameProviderService {
  var acceptsUrl: Boolean = false
  var dfToReturn: DataFrame = DefaultDataFrame(StructType.empty, Iterator.empty)
  var acceptsCalled: Boolean = false
  var getDataFrameCalled: Boolean = false
  var urlChecked: String = null

  override def accepts(dataFrameUrl: String): Boolean = {
    acceptsCalled = true
    acceptsUrl
  }

  override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                           (implicit ctx: ServerContext): DataFrame = {
    getDataFrameCalled = true
    urlChecked = dataFrameUrl
    dfToReturn
  }

  override def toString: String = s"MockDataFrameProviderServiceForDirectory($name)"
}


// --- 模拟对象 (必需的依赖) ---

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
 * 模拟 GetStreamHandler (用于测试链式调用)
 */
class MockGetStreamHandler(name: String = "OldHandler") extends GetStreamHandler {
  var doGetStreamCalled = false
  var requestReceived: DftpGetStreamRequest = null
  override def accepts(request: DftpGetStreamRequest): Boolean = true // 总是接受
  override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    doGetStreamCalled = true
    requestReceived = request
  }
}



/**
 * 模拟 DftpGetStreamResponse，用于捕获 sendError 和 sendDataFrame
 */
class MockDftpGetStreamResponse extends DftpGetStreamResponse {
  var errorSent = false
  var errorCode = 0
  var message = ""
  var dataFrameSent: DataFrame = null

  override def sendError(errorCode: Int, message: String): Unit = {
    errorSent = true
    this.errorCode = errorCode
    this.message = message
    // 实际的 sendError 会抛异常, 我们在这里模拟
    throw new RuntimeException(s"Mocked sendError: $errorCode: $message")
  }
  override def sendDataFrame(dataFrame: DataFrame): Unit = {
    dataFrameSent = dataFrame
  }
}

// 模拟各种 DftpGetStreamRequest
class MockDftpGetStreamRequest(name: String) extends DftpGetStreamRequest {
  override def getUserPrincipal(): UserPrincipal = MockUser
}
class MockDftpGetPathStreamRequest(op: TransformOp) extends DftpGetPathStreamRequest {
  override def getRequestPath(): String = op.sourceUrlList.headOption.getOrElse("")
  override def getRequestURL(): String = getRequestPath()
  override def getTransformOp(): TransformOp = op
  override def getUserPrincipal(): UserPrincipal = MockUser
}
class MockDacpGetBlobStreamRequest(id: String) extends DacpGetBlobStreamRequest {
  override def getBlobId(): String = id
  override def getUserPrincipal(): UserPrincipal = MockUser
}

/**
 * 模拟 TransformOp，用于测试 execute
 */
class MockTransformOp(name: String, dfToReturn: DataFrame) extends TransformOp {
  var executeCalled = false
  var executeContext: ExecutionContext = null

  override def execute(ctx: ExecutionContext): DataFrame = {
    this.executeCalled = true
    this.executeContext = ctx
    // 模拟调用 loadSourceDataFrame
    ctx.loadSourceDataFrame("/test/data")
    dfToReturn
  }
  override def operationType: String = "MockOp"
  override def toJson: JSONObject = new JSONObject().put("type", "MockOp").put("name", name)

  override var inputs: Seq[TransformOp] = Seq()
}


