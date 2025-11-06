/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 10:55
 * @Modified By:
 */
package link.rdcn.catalog

import link.rdcn.server.module.ObjectHolder
import link.rdcn.server.{Anchor, CrossModuleEvent, EventHandler, ServerContext}
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, StructType}
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertSame, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * 模拟一个 Anchor，用于捕获被 hook 的 EventHandler
 */
class MockAnchor extends Anchor {
  var hookedHandler: EventHandler = null

  // 模拟 Anchor 的 hook(EventHandler) 方法
  override def hook(service: EventHandler): Unit = {
    this.hookedHandler = service
  }

  // 提供一个空实现以满足 trait
  override def hook(service: link.rdcn.server.EventSource): Unit = {}

}



/**
 * 模拟一个不相关的事件，用于测试 'accepts' 方法
 */
class OtherMockEvent extends CrossModuleEvent

class CatalogServiceModuleTest {

  private var mockService: CatalogService = _
  private var moduleToTest: CatalogServiceModule = _
  private var mockAnchor: MockAnchor = _
  private var hookedEventHandler: EventHandler = _

  @BeforeEach
  def setUp(): Unit = {
    // 1. 准备: 创建模拟服务和被测模块
    mockService = new CatalogService {
      override def getDataSetMetaData(datasetName: String, model: Model): Unit =
        model.add(MockCatalogData.getMockModel)

      override def getDataFrameMetaData(dataFrameName: String, model: Model): Unit =
        model.add(MockCatalogData.getMockModel)

      override def getDocument(dataFrameName: String): DataFrameDocument =
        if (dataFrameName == "my_table") MockCatalogData.mockDoc else throw new NoSuchElementException("Doc not found")

      override def getStatistics(dataFrameName: String): DataFrameStatistics =
        if (dataFrameName == "my_table") MockCatalogData.mockStats else throw new NoSuchElementException("Stats not found")

      override def getSchema(dataFrameName: String): Option[StructType] =
        if (dataFrameName == "my_table") Some(MockCatalogData.mockSchema) else None

      override def getDataFrameTitle(dataFrameName: String): Option[String] =
        if (dataFrameName == "my_table") Some(MockCatalogData.mockTitle) else None

      override def accepts(request: CatalogServiceRequest): Boolean = true

      /**
       * 列出所有数据集名称
       *
       * @return java.util.List[String]
       */
      override def listDataSetNames(): List[String] = List("DataSetNames")

      /**
       * 列出指定数据集下的所有数据帧名称
       *
       * @param dataSetId 数据集 ID
       * @return java.util.List[String]
       */
      override def listDataFrameNames(dataSetId: String): List[String] = List("DataFrameNames")
    }
    moduleToTest = new CatalogServiceModule(mockService)
    mockAnchor = new MockAnchor()
    val mockContext = new MockServerContext()

    // 2. 执行: 调用 init 方法，这将 hook EventHandler
    moduleToTest.init(mockAnchor, mockContext)

    // 3. 提取: 获取被 hook 的 EventHandler 实例
    hookedEventHandler = mockAnchor.hookedHandler
    assertNotNull(hookedEventHandler, "init() 方法未能向 Anchor 注册 EventHandler")
  }

  /**
   * 测试 EventHandler 是否正确实现了 'accepts' 方法
   */
  @Test
  def testEventHandlerAcceptsLogic(): Unit = {
    val validEvent = RequireCatalogServiceEvent(new ObjectHolder[CatalogService])
    val invalidEvent = new OtherMockEvent()

    // 遵守 [2025-09-26] 规范：(expected, actual, message)
    assertTrue(hookedEventHandler.accepts(validEvent),
      "EventHandler 应接受 RequireCatalogServiceEvent")

    assertFalse(hookedEventHandler.accepts(invalidEvent),
      "EventHandler 不应接受其他类型的事件")
  }

  /**
   * 测试 EventHandler 是否正确实现了 'doHandleEvent' 方法
   * 验证它是否注入了*正确的* CatalogService 实例
   */
  @Test
  def testEventHandlerInjectsCorrectService(): Unit = {
    // 1. 准备: 创建一个空的 holder 和事件
    val serviceHolder = new ObjectHolder[CatalogService]
    val event = RequireCatalogServiceEvent(serviceHolder)

    // 2. 执行: 处理事件
    hookedEventHandler.doHandleEvent(event)

    // 3. 验证: 使用 invoke 代替 get
    val isCorrectServiceInjected = serviceHolder.invoke(
      run = (injectedService: CatalogService) => {
        // 遵守 [2025-09-26] 规范：(expected, actual, message)
        // 验证注入的实例是否*正是*我们传入构造函数的那个模拟实例
        assertSame(mockService, injectedService,
          "注入的服务实例与构造函数中传入的实例不一致")
        true // 表明 'run' 分支被成功执行
      },
      onNull = {
        false // 表明 'onNull' 分支被执行
      }
    )

    // 遵守 [2025-09-26] 规范：(expected, actual, message)
    assertTrue(isCorrectServiceInjected,
      "serviceHolder.invoke 的 'run' 分支没有被执行，表明 holder 为空")
  }

  /**
   * 测试 EventHandler 在处理不相关事件时不会崩溃
   */
  @Test
  def testEventHandlerHandlesOtherEventsGracefully(): Unit = {
    val otherEvent = new OtherMockEvent()

    // 简单地调用它，不应该抛出任何异常
    hookedEventHandler.doHandleEvent(otherEvent)
  }

  /**
   * 测试 destroy 方法是否可以被安全调用（无操作）
   */
  @Test
  def testDestroyMethod(): Unit = {
    // 只是为了覆盖率，确保它不会抛出异常
    moduleToTest.destroy()
  }
}
