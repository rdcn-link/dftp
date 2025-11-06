/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 18:28
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.server.{Anchor, CrossModuleEvent, EventHandler, EventSource, ServerContext}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.UserPrincipal
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * 针对 DataFrameProviderModule 的单元测试
 */
class DataFrameProviderModuleTest {

  private var mockOldService: MockDataFrameProviderService = _
  private var mockInnerService: MockDataFrameProviderService = _
  private var moduleToTest: DataFrameProviderModule = _
  private var mockAnchor: MockFlowForProvider = _
  private var hookedEventHandler: EventHandler = _

  // 模拟的 DataFrame 实例，用于验证
  private val innerDF: DataFrame = DefaultDataFrame(StructType.empty.add("inner", StringType), Seq(Row("inner")).iterator)
  private val oldDF: DataFrame = DefaultDataFrame(StructType.empty.add("old", StringType), Seq(Row("old")).iterator)

  // 模拟一个 ServerContext
  implicit private val mockContext: ServerContext = new MockServerContextForProvider()

  @BeforeEach
  def setUp(): Unit = {
    // 准备: 创建两个不同的模拟服务
    mockOldService = new MockDataFrameProviderService("OldService")
    mockInnerService = new MockDataFrameProviderService("InnerService")

    // 准备: 创建被测模块，将 'innerService' 传入构造函数
    moduleToTest = new DataFrameProviderModule(mockInnerService)
    mockAnchor = new MockFlowForProvider()

    // 执行: 调用 init 方法，这将 hook EventHandler
    moduleToTest.init(mockAnchor, mockContext)

    // 提取: 获取被 hook 的 EventHandler 实例
    hookedEventHandler = mockAnchor.hookedHandler
    assertNotNull(hookedEventHandler, "init() 方法未能向 Anchor 注册 EventHandler")
  }

  /**
   * 测试 EventHandler 是否正确实现了 'accepts' 方法
   */
  @Test
  def testEventHandlerAcceptsLogic(): Unit = {
    val validEvent = RequireDataFrameProviderEvent(new ObjectHolder[DataFrameProviderService])
    val invalidEvent = new OtherMockEventForProvider()

    assertTrue(hookedEventHandler.accepts(validEvent),
      "EventHandler 应接受 RequireDataFrameProviderEvent")

    assertFalse(hookedEventHandler.accepts(invalidEvent),
      "EventHandler 不应接受其他类型的事件")
  }

  /**
   * 测试链式逻辑: InnerService 接受，OldService 拒绝
   * 预期: InnerService 被调用
   */
  @Test
  def testChainingLogic_InnerServiceAccepts(): Unit = {
    // 准备:
    // InnerService (构造函数中的) 接受 URL
    mockInnerService.acceptsUrl = true
    mockInnerService.dfToReturn = innerDF
    // OldService (已存在的) 不接受 URL
    mockOldService.acceptsUrl = false
    mockOldService.dfToReturn = oldDF

    // 模拟事件: 将 'mockOldService' 放入 holder
    val holder = new ObjectHolder[DataFrameProviderService]()
    holder.set(mockOldService)
    val event = RequireDataFrameProviderEvent(holder)

    // 执行: 处理事件，创建链式服务
    hookedEventHandler.doHandleEvent(event)

    // 提取: 获取新的链式服务
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    val testUrl = "http://inner.com/data"

    // 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(testUrl), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 验证 getDataFrame() 链 (InnerService 优先)
    val resultDF = chainedService.getDataFrame(testUrl, MockUserForProvider)

    assertEquals(innerDF, resultDF, "链式 getDataFrame() 应返回 InnerService 的 DataFrame")

    // 验证调用
    assertTrue(mockInnerService.getDataFrameCalled, "InnerService.getDataFrame 应被调用")
    assertEquals(testUrl, mockInnerService.urlChecked, "InnerService 检查了错误的 URL")

    assertFalse(mockOldService.getDataFrameCalled, "OldService.getDataFrame 不应被调用")
  }

  /**
   * 测试链式逻辑: InnerService 拒绝，OldService 接受
   * 预期: OldService 被调用
   */
  @Test
  def testChainingLogic_OldServiceAccepts(): Unit = {
    // 准备:
    // InnerService (构造函数中的) 不接受 URL
    mockInnerService.acceptsUrl = false
    // OldService (已存在的) 接受 URL
    mockOldService.acceptsUrl = true
    mockOldService.dfToReturn = oldDF

    // 模拟事件
    val holder = new ObjectHolder[DataFrameProviderService]()
    holder.set(mockOldService)
    val event = RequireDataFrameProviderEvent(holder)

    // 执行
    hookedEventHandler.doHandleEvent(event)

    // 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    val testUrl = "http://old.com/data"

    // 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(testUrl), "链式 accepts() 应返回 true (因为 OldService 接受)")

    // 验证 getDataFrame() 链 (Inner 失败, Old 成功)
    val resultDF = chainedService.getDataFrame(testUrl, MockUserForProvider)

    assertEquals(oldDF, resultDF, "链式 getDataFrame() 应返回 OldService 的 DataFrame")

    // 验证调用
    assertFalse(mockInnerService.getDataFrameCalled, "InnerService.getDataFrame 不应被调用")

    assertTrue(mockOldService.getDataFrameCalled, "OldService.getDataFrame 应被调用")
    assertEquals(testUrl, mockOldService.urlChecked, "OldService 检查了错误的 URL")
  }

  /**
   * 测试链式逻辑: 两个服务都拒绝
   * 预期: 抛出 DataFrameNotFoundException
   */
  @Test
  def testChainingLogic_NoServiceAccepts(): Unit = {
    // 准备: 两个服务都不接受
    mockInnerService.acceptsUrl = false
    mockOldService.acceptsUrl = false

    // 模拟事件
    val holder = new ObjectHolder[DataFrameProviderService]()
    holder.set(mockOldService)
    val event = RequireDataFrameProviderEvent(holder)

    // 执行
    hookedEventHandler.doHandleEvent(event)

    // 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    val testUrl = "http://none.com/data"

    // 验证 accepts() 链
    assertFalse(chainedService.accepts(testUrl), "链式 accepts() 应返回 false (因为两者都拒绝)")

    // 验证 getDataFrame() 链 (抛出异常)
    val ex = assertThrows(classOf[DataFrameNotFoundException], () => {
      chainedService.getDataFrame(testUrl, MockUserForProvider)
      ()
    }, "链式 getDataFrame() 在两者都拒绝时应抛出 DataFrameNotFoundException")

    assertTrue(ex.getMessage.contains(testUrl), "异常消息应包含未找到的 URL")

    // 验证调用
    assertFalse(mockInnerService.getDataFrameCalled, "InnerService.getDataFrame 不应被调用")
    assertFalse(mockOldService.getDataFrameCalled, "OldService.getDataFrame 不应被调用")
  }

  /**
   * 测试当 Holder 中 'old' 为 null 时的链式逻辑
   * 预期: 只有 InnerService 工作
   */
  @Test
  def testChainingLogic_HolderInitiallyEmpty(): Unit = {
    // 准备:
    mockInnerService.acceptsUrl = true
    mockInnerService.dfToReturn = innerDF

    // 模拟事件: Holder 为空 (old = null)
    val holder = new ObjectHolder[DataFrameProviderService]()
    val event = RequireDataFrameProviderEvent(holder)

    // 执行
    hookedEventHandler.doHandleEvent(event)

    // 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    val testUrl = "http://inner.com/data"

    // 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(testUrl), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 验证 getDataFrame() 链 (InnerService 优先)
    val resultDF = chainedService.getDataFrame(testUrl, MockUserForProvider)

    assertEquals(innerDF, resultDF, "链式 getDataFrame() 应返回 InnerService 的 DataFrame")

    // 验证调用
    assertTrue(mockInnerService.getDataFrameCalled, "InnerService.getDataFrame 应被调用 (即使 old 为 null)")
    assertEquals(testUrl, mockInnerService.urlChecked, "InnerService 检查了错误的 URL")
  }
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

