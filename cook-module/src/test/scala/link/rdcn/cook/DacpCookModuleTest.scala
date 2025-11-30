/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:10
 * @Modified By:
 */
package link.rdcn.cook

import link.rdcn._
import link.rdcn.dacp.cook.{DacpCookModule, DacpCookStreamRequest}
import link.rdcn.dacp.user.{PermissionService, RequirePermissionServiceEvent}
import link.rdcn.operation.SourceOp
import link.rdcn.server._
import link.rdcn.server.module._
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DefaultDataFrame, Row, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class DacpCookModuleTest {

  private var moduleToTest: DacpCookModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  implicit private var mockContext: ServerContext = _

  // Holders
  private var dataFrameHolder: Workers[DataFrameProviderService] = _
  private var permissionHolder: Workers[PermissionService] = _
  private var getStreamParserHolder: Workers[ParseRequestMethod] = _
  private var getStreamHandlerHolder: FilteredGetStreamMethods = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new DacpCookModule()
    mockAnchor = new MockAnchor()

    mockContext = new MockServerContext()

    // 执行 init()
    moduleToTest.init(mockAnchor, mockContext)
    mockEventHub = new MockEventHub(mockAnchor.hookedEventHandlers)

    // 验证 EventHandler 和 EventSource 被 hook
    assertEquals(1, mockAnchor.hookedEventHandlers.length, "init() 应 hook 1 个 EventHandler")
    assertNotNull(mockAnchor.hookedEventSource, "init() 应 hook 1 个 EventSource")

    // 触发 EventSource 来捕获用于依赖注入的 holder
    mockAnchor.hookedEventSource.init(mockEventHub)

    dataFrameHolder = mockEventHub.eventsFired.find(_.isInstanceOf[CollectDataFrameProviderEvent]).get
      .asInstanceOf[CollectDataFrameProviderEvent].holder
    permissionHolder = mockEventHub.eventsFired.find(_.isInstanceOf[RequirePermissionServiceEvent]).get
      .asInstanceOf[RequirePermissionServiceEvent].holder

    // 触发 EventHandler 来捕获用于链式调用的 holder
    getStreamParserHolder = new Workers[ParseRequestMethod]
    mockEventHub.fireEvent(new CollectParseRequestMethodEvent(getStreamParserHolder))

    getStreamHandlerHolder = new FilteredGetStreamMethods
    mockEventHub.fireEvent(new CollectGetStreamMethodEvent(getStreamHandlerHolder))
  }

  /**
   * 测试 init 是否正确触发了 CollectDataFrameProviderEvent 和 RequirePermissionServiceEvent
   */
  @Test
  def testInit_FiresAndHooksEvents(): Unit = {
    assertEquals(4, mockEventHub.eventsFired.length, "EventSource.init() 应触发 4 个事件")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[CollectDataFrameProviderEvent]), "CollectDataFrameProviderEvent 未被触发")
    assertTrue(mockEventHub.eventsFired.exists(_.isInstanceOf[RequirePermissionServiceEvent]), "RequirePermissionServiceEvent 未被触发")
  }

  /**
   * 测试 GetStreamRequestParser 是否能正确解析 COOK_TICKET
   */
  @Test
  def testParser_ParsesCookTicket(): Unit = {
    val chainedParser = getStreamParserHolder.work(runMethod = s => s, onNull = null)
    assertNotNull(chainedParser, "GetStreamRequestParser 未被设置")

    // 准备 Ticket
    val COOK_TICKET: Byte = 3
    val json = """{"type": "SourceOp", "dataFrameName": "/my/data"}"""
    val jsonBytes = json.getBytes(StandardCharsets.UTF_8)

    val bb = ByteBuffer.allocate(1 + 4 + jsonBytes.length)
    bb.put(COOK_TICKET)
    bb.putInt(jsonBytes.length)
    bb.put(jsonBytes)
    val ticketBytes = bb.array()

    // 执行
    val request = chainedParser.parse(ticketBytes, MockUser)

    // 验证
    assertTrue(request.isInstanceOf[DacpCookStreamRequest], "解析器应返回 DacpCookStreamRequest")
    val cookRequest = request.asInstanceOf[DacpCookStreamRequest]
    val tree = cookRequest.getTransformTree
    assertTrue(tree.isInstanceOf[SourceOp], "解析的 TransformTree 类型不正确")
    assertEquals("/my/data", tree.asInstanceOf[SourceOp].dataFrameUrl, "解析的 TransformTree 内容不正确")
  }

  /**
   * 测试 GetStreamRequestParser 是否将非 COOK_TICKET 传递给 'old' 解析器
   */
  @Test
  def testParser_ChainsToOld(): Unit = {
    // 准备: 创建一个 "old" 解析器并注入
    val mockOldParser = new MockGetStreamRequestParser()
    getStreamParserHolder.add(mockOldParser) // 覆盖
    mockEventHub.fireEvent(new CollectParseRequestMethodEvent(getStreamParserHolder)) // 重新触发事件

    val chainedParser = getStreamParserHolder.work(runMethod = s => s, onNull = null)

    // 准备一个非 COOK_TICKET (e.g., type 1)
    val otherTicketBytes = Array[Byte](1, 0, 0, 0, 0)

    // 执行
    val request = chainedParser.parse(otherTicketBytes, MockUser)

    // 验证
    assertTrue(mockOldParser.parseCalled, "'old' 解析器的 parse() 方法应被调用")
    assertEquals(mockOldParser.requestToReturn, request, "应返回 'old' 解析器的结果")
  }

  /**
   * 测试 GetStreamHandler 是否将非 DacpCookStreamRequest 传递给 'old' 处理器
   */
  @Test
  def testHandler_ChainsToOld(): Unit = {
    // 准备: 创建一个 "old" 处理器并注入
    val mockOldHandler = new MockGetStreamHandler()
    getStreamHandlerHolder.addMethod(mockOldHandler) // 覆盖
    mockEventHub.fireEvent(new CollectGetStreamMethodEvent(getStreamHandlerHolder)) // 重新触发

    // 准备一个非 DacpCookStreamRequest
    val otherRequest = new MockDftpGetStreamRequest("NotCook")

    // 执行
    getStreamHandlerHolder.handle(otherRequest, null) // response 在此路径中不被使用

    // 验证
    assertTrue(mockOldHandler.doGetStreamCalled, "'old' 处理器的 doGetStream() 方法应被调用")
  }

  /**
   * 测试 GetStreamHandler (DacpCookRequest) 的“黄金路径”
   * 假设: PYTHON_HOME 已设置, 权限被授予, DataFrame 存在
   */
  @Test
  @Disabled("集成测试：此测试要求在运行环境中设置 'PYTHON_HOME' 环境变量")
  def testHandler_Execute_HappyPath(): Unit = {

    // 准备: 注入模拟服务
    val mockDf = DefaultDataFrame(StructType.empty.add("result", StringType), Seq(Row("Success")).iterator)
    dataFrameHolder.add(new MockDataFrameProviderService(Some(mockDf)))
    permissionHolder.add(new MockPermissionService(allowAccess = true))

    // 准备: 模拟请求
    val mockTree = new MockTransformOp("test-tree", mockDf)
    val request = new MockDacpCookStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    // 执行
    getStreamHandlerHolder.handle(request, response)

    // 验证
    assertTrue(mockTree.executeCalled, "TransformTree.execute 应被调用")
    assertNotNull(mockTree.executeContext, "execute 应收到一个 FlowExecutionContext")
    assertFalse(response.errorSent, "Happy path 不应发送错误")
    assertEquals(mockDf, response.dataFrameSent, "Response.sendDataFrame 未收到正确的 DataFrame")
  }

  /**
   * 测试 FlowExecutionContext - 权限被拒绝
   */
  @Test
  @Disabled("集成测试：此测试要求在运行环境中设置 'PYTHON_HOME' 环境变量")
  def testHandler_Execute_PermissionDenied(): Unit = {

    // 准备: 注入*拒绝*的 PermissionService
    dataFrameHolder.add(new MockDataFrameProviderService(None)) // (不应被调用)
    permissionHolder.add(new MockPermissionService(allowAccess = false))

    // 准备: 模拟请求 (MockTransformOp 会调用 loadSourceDataFrame)
    val mockTree = new MockTransformOp("test-tree", DefaultDataFrame(StructType.empty, Iterator.empty))
    val request = new MockDacpCookStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    // 执行并验证
    // (doGetStream -> execute -> loadSourceDataFrame -> checkPermission -> throw IllegalAccessException -> catch -> sendError(403))
    val ex = assertThrows(classOf[RuntimeException], () => {
      getStreamHandlerHolder.handle(request, response)
      ()
    }, "doGetStream 应抛出由 sendError(403) 模拟的 RuntimeException")

    assertTrue(response.errorSent, "response.sendError(403) 应被调用")
    assertEquals(403, response.errorCode, "错误码应为 403 (Forbidden)")
    assertTrue(response.message.contains("permission to access"), "错误消息应指明权限问题")
  }

  /**
   * 测试 FlowExecutionContext - DataFrame 未找到
   */
  @Test
  @Disabled("集成测试：此测试要求在运行环境中设置 'PYTHON_HOME' 环境变量")
  def testHandler_Execute_DataFrameNotFound(): Unit = {

    // 准备: 注入允许的 PermissionService, 但注入一个*失败*的 DataFrameProvider
    dataFrameHolder.add(new MockDataFrameProviderService(None)) // (将抛出 DataFrameNotFoundException)
    permissionHolder.add(new MockPermissionService(allowAccess = true))

    // 准备: 模拟请求
    val mockTree = new MockTransformOp("test-tree", DefaultDataFrame(StructType.empty, Iterator.empty))
    val request = new MockDacpCookStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    // 执行并验证
    // (doGetStream -> execute -> loadSourceDataFrame -> getDataFrame -> throw DataFrameNotFoundException -> catch -> sendError(404))
    val ex = assertThrows(classOf[RuntimeException], () => {
      getStreamHandlerHolder.handle(request, response)
      ()
    }, "doGetStream 应抛出由 sendError(404) 模拟的 RuntimeException")

    assertTrue(response.errorSent, "response.sendError(404) 应被调用")
    assertEquals(404, response.errorCode, "错误码应为 404 (Not Found)")
    assertTrue(response.message.contains("not Found"), "错误消息应指明 DataFrame 未找到")
  }
}