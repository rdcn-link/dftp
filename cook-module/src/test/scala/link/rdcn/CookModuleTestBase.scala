package link.rdcn

import link.rdcn.dacp.cook.DacpCookStreamRequest
import link.rdcn.dacp.optree.{FileRepositoryBundle, FlowExecutionContext, OperatorRepository, TransformFunctionWrapper, TransformerNode}
import link.rdcn.dacp.recipe.FlowNode
import link.rdcn.dacp.user.{DataOperationType, PermissionService}
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.server.module.DataFrameProviderService
import link.rdcn.server._
import link.rdcn.struct._
import link.rdcn.user.{Credentials, UserPrincipal}
import org.json.JSONObject

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

/**
 * 模拟 EventHub，用于捕获所有被触发的事件
 */
class MockEventHub(hookedEventHandlers: ArrayBuffer[EventHandler]) extends EventHub {
  val eventsFired = new ArrayBuffer[CrossModuleEvent]()
  override def fireEvent(event: CrossModuleEvent): Unit = {
    hookedEventHandlers.filter(_.accepts(event)).foreach(_.doHandleEvent(event))
    eventsFired.append(event)
  }
}

/**
 * 模拟 Anchor，用于捕获 KernelModule hook 的 EventSource 和 EventHandler
 */
class MockAnchor extends Anchor {
  var hookedEventSource: EventSource = null
  var hookedHandler: EventHandler = null
  val hookedEventHandlers = new ArrayBuffer[EventHandler]()

  override def hook(service: EventSource): Unit = {
    this.hookedEventSource = service
  }
  override def hook(service: EventHandler): Unit = {
    this.hookedEventHandlers.append(service)
    hookedHandler = service
  }
}

/**
 * 模拟一个 ServerContext
 */
class MockServerContext extends ServerContext {
  override def getHost(): String = "mock-host"
  override def getPort(): Int = 1234
  override def getProtocolScheme(): String = "dftp"
  override def getDftpHome(): Option[String] = Some("./mock-dftp-home")
}

/**
 * 模拟一个 UserPrincipal
 */
case object MockUser extends UserPrincipal {
  def getName: String = "MockUser"
}

/**
 * 模拟 GetStreamRequestParser (用于测试链式调用)
 */
class MockGetStreamRequestParser extends GetStreamRequestParser {
  var parseCalled = false
  val requestToReturn: DftpGetStreamRequest = new MockDftpGetStreamRequest("OldRequest")
  override def accepts(token: Array[Byte]): Boolean = true
  override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    parseCalled = true
    requestToReturn
  }
}

/**
 * 模拟 GetStreamHandler (用于测试链式调用)
 */
class MockGetStreamHandler extends GetStreamHandler {
  var doGetStreamCalled = false
  override def accepts(request: DftpGetStreamRequest): Boolean = true
  override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    doGetStreamCalled = true
  }
}

/**
 * 模拟 DataFrameProviderService
 */
class MockDataFrameProviderService(dfToReturn: Option[DataFrame]) extends DataFrameProviderService {
  var getDataFrameCalled = false
  override def accepts(dataFrameUrl: String): Boolean = true // 假设接受
  override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                           (implicit ctx: ServerContext): DataFrame = {
    getDataFrameCalled = true
    dfToReturn.getOrElse(throw new DataFrameNotFoundException(s"Mock DF $dataFrameUrl not found"))
  }
}

/**
 * 模拟 PermissionService
 */
class MockPermissionService(allowAccess: Boolean) extends PermissionService {
  var checkPermissionCalled = false
  override def accepts(user: UserPrincipal): Boolean = true // 假设接受
  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
    checkPermissionCalled = true
    allowAccess
  }
}

class MockPermissionServiceForPermissionServiceTest(name: String) extends PermissionService {
  var acceptsUser: Boolean = false
  var permissionResult: Boolean = false
  var acceptsCalled: Boolean = false
  var checkPermissionCalled: Boolean = false
  var userChecked: UserPrincipal = null
  var dataFrameChecked: String = null
  var opsChecked: List[DataOperationType] = null

  override def accepts(user: UserPrincipal): Boolean = {
    acceptsCalled = true
    acceptsUser
  }

  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
    checkPermissionCalled = true
    userChecked = user
    dataFrameChecked = dataFrameName
    opsChecked = opList
    permissionResult
  }

  override def toString: String = s"MockPermissionService($name)"
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
class MockDacpCookStreamRequest(tree: TransformOp) extends DacpCookStreamRequest {
  override def getTransformTree: TransformOp = tree
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
    ctx.asInstanceOf[FlowExecutionContext].loadSourceDataFrame("/test/data")
    dfToReturn
  }
  override def operationType: String = "MockOp"
  override def toJson: JSONObject = new JSONObject().put("type", "MockOp").put("name", name)

  override var inputs: Seq[TransformOp] = Seq()
}

/**
 * 模拟的 TransformerNode，用于测试 'contain' 和 'release' 逻辑
 */
class MockTransformerNode(transformFunctionWrapper: TransformFunctionWrapper) extends TransformerNode(transformFunctionWrapper) {
  // 用于测试 'contain' 逻辑
  var childToContain: TransformOp = _

  // 用于测试 'release' 逻辑
  val released = new AtomicBoolean(false)

  // --- TransformerNode 抽象方法实现 ---
  override def contain(op: TransformerNode): Boolean = op == childToContain
  override def release(): Unit = released.set(true)

  // --- TransformOp 抽象方法实现 ---
  override def execute(context: ExecutionContext): DataFrame =
    DefaultDataFrame(StructType.empty,Iterator.empty) // 仅为模拟，不执行
  def getInputs(): Seq[TransformOp] = Seq.empty // 仅为模拟
}

/**
 * MockFlowExecutionContext，用于测试 FlowExecutionContext trait
 */
class MockFlowExecutionContext extends FlowExecutionContext {
  // --- 抽象方法实现 ---
  override def fairdHome: String = "/mock/faird/home"
  override def pythonHome: String = {
    // 尝试查找系统中的 python.home，否则返回模拟路径
    // 这是 JEP 集成测试所必需的
    val pyHome = System.getProperty("python.home")
    if (pyHome != null) pyHome else "/mock/python/home"
  }

  override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = None
  override def getRepositoryClient(): Option[OperatorRepository] = None
  override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
}

/**
 * 用于测试 TransformerNode.release() 的模拟类
 */
class MockReleasableTransformerNode(name: String) extends TransformerNode(null) {
  val released = new AtomicBoolean(false)
  override def release(): Unit = released.set(true)
  override def toJson: JSONObject = new JSONObject().put("type", "MockTransformerNode").put("name", name)
}

/**
 * 模拟的 TransformFunctionWrapper
 */
class MockTransformFunctionWrapper(name: String, dfToReturn: DataFrame) extends TransformFunctionWrapper {
  var applyCalledWith: Seq[DataFrame] = null
  var applyCalledContext: FlowExecutionContext = null

  override def toJson: JSONObject = new JSONObject().put("type", "MockFunction").put("name", name)

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    this.applyCalledWith = inputs
    this.applyCalledContext = ctx
    dfToReturn
  }
}

/**
 * 模拟的 TransformOp
 */
case class MockTransformOpForTransformTree(name: String, dfToReturn: DataFrame) extends TransformOp {
  var executeCalled = false
  var executeContext: ExecutionContext = null

  override def execute(ctx: ExecutionContext): DataFrame = {
    this.executeCalled = true
    this.executeContext = ctx
    dfToReturn
  }
  override def operationType: String = "MockOp"
  override def toJson: JSONObject = new JSONObject().put("type", "MockOp").put("name", name)

  override var inputs: Seq[TransformOp] = Seq.empty
}

/**
 * 模拟的 FlowExecutionContext
 */
class MockFlowExecutionContextForTransformTree(asyncEnabled: Boolean = false) extends FlowExecutionContext {
  var registeredFutures = new ArrayBuffer[TransformOp]()
  var remoteDataFrames = Map[String, DataFrame]()

  override def fairdHome: String = "/mock/faird/home"
  override def pythonHome: String = "/mock/python/home"
  isAsyncEnabled = asyncEnabled

  override def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame], thread: Thread): Unit = {
    registeredFutures.append(transformOp)
  }

  override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = {
    remoteDataFrames.get(baseUrl + path)
  }

  override def getRepositoryClient(): Option[OperatorRepository] = None
  override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
}

/**
 * 模拟 Flow.scala 所需的依赖
 */
case class SourceNode(path: String) extends FlowNode
case class FifoFileBundleFlowNode(bundle: FileRepositoryBundle) extends FlowNode
case class OtherNode(name: String) extends FlowNode

/**
 * 模拟一个不相关的事件
 */
class OtherMockEvent extends CrossModuleEvent