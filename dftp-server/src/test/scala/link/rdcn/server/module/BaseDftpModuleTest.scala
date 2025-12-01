/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/30 18:25
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.operation.SourceOp
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class BaseDftpModuleTest {

  private var moduleToTest: BaseDftpModule = _
  private var mockAnchor: MockAnchor = _
  private var mockEventHub: MockEventHub = _
  implicit private var mockContext: ServerContext = _

  // Handlers
  private var parserEventHandler: EventHandler = _
  private var streamEventHandler: EventHandler = _

  // Holders
  private var dataFrameHolder: Workers[DataFrameProviderService] = _

  @TempDir
  var tempDirectory: Path = _

  @BeforeEach
  def setUp(): Unit = {
    moduleToTest = new BaseDftpModule()
    mockAnchor = new MockAnchor()
    mockEventHub = new MockEventHub()
    mockContext = new MockServerContext()

    // 1. 执行 init()
    moduleToTest.init(mockAnchor, mockContext)

    // 2. 验证 EventSource 和 EventHandlers 被 hook
    assertNotNull(mockAnchor.hookedEventSource, "init() 应 hook 1 个 EventSource")
    assertEquals(2, mockAnchor.hookedEventHandlers.length, "init() 应 hook 2 个 EventHandler")

    // 3. 触发 EventSource 来捕获 dataFrameHolder
    mockAnchor.hookedEventSource.init(mockEventHub)
    val event = mockEventHub.eventsFired.find(_.isInstanceOf[CollectDataFrameProviderEvent]).get
    dataFrameHolder = event.asInstanceOf[CollectDataFrameProviderEvent].holder
    assertNotNull(dataFrameHolder, "EventSource 未能正确触发 CollectDataFrameProviderEvent")

    // 4. 提取两个 EventHandler
    parserEventHandler = mockAnchor.hookedEventHandlers.find(
      _.accepts(new CollectParseRequestMethodEvent(null))
    ).get
    streamEventHandler = mockAnchor.hookedEventHandlers.find(
      _.accepts(new CollectGetStreamMethodEvent(null))
    ).get

    assertNotNull(parserEventHandler, "未能找到 GetStreamRequestParserEvent 处理器")
    assertNotNull(streamEventHandler, "未能找到 RequireGetStreamHandlerEvent 处理器")
  }

  // --- GetStreamRequestParser (Ticket 解析) 测试 ---

  /**
   * 准备一个 Ticket 字节数组
   *
   * @param typeId  1 = BLOB, 2 = URL
   * @param content 票据内容
   */
  private def createTicketBytes(typeId: Byte, content: String): Array[Byte] = {
    val jsonBytes = content.getBytes(StandardCharsets.UTF_8)
    val bb = ByteBuffer.allocate(1 + 4 + jsonBytes.length)
    bb.put(typeId)
    bb.putInt(jsonBytes.length)
    bb.put(jsonBytes)
    bb.array()
  }

  @Test
  def testParser_BLOB_TICKET(): Unit = {
    val holder = new Workers[ParseRequestMethod]()
    parserEventHandler.doHandleEvent(new CollectParseRequestMethodEvent(holder))
    val parser = holder.work(runMethod = s => s, onFail = null)
    assertNotNull(parser, "Parser 未被注入")

    val blobId = "my-blob-id-123"
    val ticketBytes = createTicketBytes(1, blobId) // 1 = BLOB_TICKET

    assertTrue(parser.accepts(ticketBytes), "Parser 应接受 BLOB_TICKET (1)")

    val request = parser.parse(ticketBytes, MockUser)

    assertTrue(request.isInstanceOf[DacpGetBlobStreamRequest], "应返回 DacpGetBlobStreamRequest")
    assertEquals(blobId, request.asInstanceOf[DacpGetBlobStreamRequest].getBlobId(), "解析的 Blob ID 不匹配")
  }

  @Test
  def testParser_URL_GET_TICKET_PartialPath(): Unit = {
    val holder = new Workers[ParseRequestMethod]()
    parserEventHandler.doHandleEvent(new CollectParseRequestMethodEvent(holder))
    val parser = holder.work(runMethod = s => s, onFail = null)

    // 2 = URL_GET_TICKET
    // 使用一个部分路径
    val json = """{"type": "SourceOp", "dataFrameName": "/my/data"}"""
    val ticketBytes = createTicketBytes(2, json)

    assertTrue(parser.accepts(ticketBytes), "Parser 应接受 URL_GET_TICKET (2)")

    val request = parser.parse(ticketBytes, MockUser)

    assertTrue(request.isInstanceOf[DftpGetPathStreamRequest], "应返回 DftpGetPathStreamRequest")
    val pathRequest = request.asInstanceOf[DftpGetPathStreamRequest]

    // 验证 UrlValidator 的 "Left" 路径 (补全 URL)
    val expectedUrl = s"${mockContext.baseUrl}/my/data"
    assertEquals("/my/data", pathRequest.getRequestPath(), "getRequestPath() 应返回部分路径")
    assertEquals(expectedUrl, pathRequest.getRequestURL(), "getRequestURL() 应返回补全的 URL")
    assertTrue(pathRequest.getTransformOp().isInstanceOf[SourceOp], "TransformOp 解析不正确")
  }

  @Test
  def testParser_URL_GET_TICKET_FullUrl(): Unit = {
    val holder = new Workers[ParseRequestMethod]()
    parserEventHandler.doHandleEvent(new CollectParseRequestMethodEvent(holder))
    val parser = holder.work(runMethod = s => s, onFail = null)

    // 2 = URL_GET_TICKET
    // 使用一个完整的 URL
    val fullUrl = "dftp://other-host:8080/data"
    val json = s"""{"type": "SourceOp", "dataFrameName": "$fullUrl"}"""
    val ticketBytes = createTicketBytes(2, json)

    val request = parser.parse(ticketBytes, MockUser)
    val pathRequest = request.asInstanceOf[DftpGetPathStreamRequest]

    // 验证 UrlValidator 的 "Right" 路径 (使用原始 URL)
    assertEquals("/data", pathRequest.getRequestPath(), "getRequestPath() 应返回 URL 中的路径部分")
    assertEquals(fullUrl, pathRequest.getRequestURL(), "getRequestURL() 应返回原始的完整 URL")
  }

  // --- GetStreamHandler (Stream 处理) 测试 ---

  private def getChainedStreamHandler(oldHandler: GetStreamMethod = null): FilteredGetStreamMethods = {
    val holder = new FilteredGetStreamMethods()
    if (oldHandler != null) {
      holder.addMethod(oldHandler)
    }
    streamEventHandler.doHandleEvent(new CollectGetStreamMethodEvent(holder))
    holder
  }

  @Test
  @Disabled("直接使用Blob会被关闭")
  def testHandler_DacpGetBlobStreamRequest_HappyPath(): Unit = {
    val blobId = "my-blob"
    val blobData = "Hello Blob".getBytes(StandardCharsets.UTF_8)

    // 1. 准备: 将 Blob 放入 Registry
    val tempFile: Path = tempDirectory.resolve("my-blob-file.txt")
    Files.write(tempFile, blobData)
    val blob = Blob.fromFile(new File(tempFile.toString))
    BlobRegistry.register(blob)

    val request = new MockDacpGetBlobStreamRequest(blobId)
    val response = new MockDftpGetStreamResponse()

    // 2. 执行
    getChainedStreamHandler().handle(request, response)

    // 3. 验证
    assertFalse(response.errorSent, "Happy path 不应发送错误")
    assertNotNull(response.dataFrameSent, "sendDataFrame 应被调用")
    assertEquals(StructType.blobStreamStructType, response.dataFrameSent.schema, "Blob Stream 的 Schema 不正确")

    // 验证内容
    val rows = response.dataFrameSent.collect()
    assertEquals(1, rows.length, "Blob Stream 应只有 1 行")
    assertEquals(blobData, rows.head.getAs[Array[Byte]](0), "Blob 数据不匹配")
  }

  @Test
  def testHandler_DacpGetBlobStreamRequest_NotFound(): Unit = {
    val request = new MockDacpGetBlobStreamRequest("non-existent-id")
    val response = new MockDftpGetStreamResponse()

    // 执行并验证
    val ex = assertThrows(classOf[RuntimeException], () => {
      getChainedStreamHandler().handle(request, response)
      ()
    }, "请求不存在的 Blob ID 应抛出异常 (由 sendError 模拟)")

    assertTrue(response.errorSent, "response.sendError(404) 应被调用")
    assertEquals(404, response.errorCode, "错误码应为 404")
  }

  @Test
  def testHandler_DftpGetPathStreamRequest_HappyPath(): Unit = {

    // 1. 准备: 注入 DataFrameProvider
    val mockDf = DefaultDataFrame(StructType.empty.add("name", StringType), Seq(Row("Success")).iterator)
    dataFrameHolder.add(new MockDataFrameProviderServiceForBase(Some(mockDf)))

    // 2. 准备请求
    val mockTree = new MockTransformOp("test-tree", mockDf)
    val request = new MockDftpGetPathStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    // 3. 执行
    getChainedStreamHandler().handle(request, response)

    // 4. 验证
    assertTrue(mockTree.executeCalled, "TransformOp.execute 应被调用")
    assertFalse(response.errorSent, "Happy path 不应发送错误")
    assertEquals(mockDf, response.dataFrameSent, "response.sendDataFrame 未收到正确的 DataFrame")
  }

  @Test
  def testHandler_DftpGetPathStreamRequest_PermissionDenied(): Unit = {

    // 1. 准备: 注入一个会抛出 IllegalAccessException 的 Provider
    val exception = new DataFrameAccessDeniedException("test-tree")
    dataFrameHolder.add(new MockDataFrameProviderServiceForBase(None, Some(exception)))

    // 2. 准备请求
    val mockTree = new MockTransformOp("test-tree", DefaultDataFrame(StructType.empty, Iterator.empty))
    val request = new MockDftpGetPathStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    // 3. 执行并验证
    val ex = assertThrows(classOf[RuntimeException], () => {
      getChainedStreamHandler().handle(request, response)
      ()
    }, "doGetStream 应抛出异常 (由 sendError(403) 模拟)")

    assertTrue(response.errorSent, "response.sendError(403) 应被调用")
    assertEquals(403, response.errorCode, "错误码应为 403 (Forbidden)")
    assertEquals("Access denied to DataFrame test-tree", response.message, "错误消息不匹配")
  }

  @Test
  def testHandler_DftpGetPathStreamRequest_NotFound_NoOldHandler(): Unit = {
    // 1. 准备: 注入一个会抛出 DataFrameNotFoundException 的 Provider
    val exception = new DataFrameNotFoundException("DF Not Found")
    dataFrameHolder.add(new MockDataFrameProviderServiceForBase(None, Some(exception)))

    // 2. 准备: *不* 注入 'old' 处理器

    // 3. 准备请求
    val mockTree = new MockTransformOp("test-tree", DefaultDataFrame(StructType.empty, Iterator.empty))
    val request = new MockDftpGetPathStreamRequest(mockTree)
    val response = new MockDftpGetStreamResponse()

    // 4. 执行并验证
    val ex = assertThrows(classOf[RuntimeException], () => {
      getChainedStreamHandler().handle(request, response)
      ()
    }, "doGetStream 应抛出异常 (由 sendError(404) 模拟)")

    assertTrue(response.errorSent, "response.sendError(404) 应被调用")
    assertEquals(404, response.errorCode, "错误码应为 404")
  }

  @Test
  def testHandler_ChainsToOld_OtherRequest(): Unit = {
    // 1. 准备: 注入 'old' 处理器
    val mockOldHandler = new MockGetStreamHandler("OldHandler")

    // 2. 准备一个 "Other" 请求 (非 Blob, 非 Path)
    val otherRequest = new MockDftpGetStreamRequest("Other")
    val response = new MockDftpGetStreamResponse()

    // 3. 执行
    getChainedStreamHandler().handle(otherRequest, response)

    // 4. 验证
    assertTrue(mockOldHandler.doGetStreamCalled, "'old' 处理器应被调用")
    assertEquals(otherRequest, mockOldHandler.requestReceived, "'old' 处理器收到的请求不匹配")
    assertFalse(response.errorSent, "不应发送错误")
  }

}
