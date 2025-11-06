/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:06
 * @Modified By:
 */
package link.rdcn.user

import link.rdcn.server.module.ObjectHolder
import link.rdcn.server.{Anchor, CrossModuleEvent, EventHandler, ServerContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertSame, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

class PermissionServiceModuleTest {

  private var mockOldService: MockPermissionService = _
  private var mockInnerService: MockPermissionService = _
  private var moduleToTest: PermissionServiceModule = _
  private var mockAnchor: MockAnchor = _
  private var hookedEventHandler: EventHandler = _

  @BeforeEach
  def setUp(): Unit = {
    // 1. 准备: 创建两个不同的模拟服务
    mockOldService = new MockPermissionService("OldService")
    mockInnerService = new MockPermissionService("InnerService")

    // 2. 准备: 创建被测模块，将 'innerService' 传入构造函数
    moduleToTest = new PermissionServiceModule(mockInnerService)
    mockAnchor = new MockAnchor()
    val mockContext = new MockServerContext()

    // 3. 执行: 调用 init 方法，这将 hook EventHandler
    moduleToTest.init(mockAnchor, mockContext)

    // 4. 提取: 获取被 hook 的 EventHandler 实例
    hookedEventHandler = mockAnchor.hookedHandler
    assertNotNull(hookedEventHandler, "init() 方法未能向 Anchor 注册 EventHandler")
  }

  /**
   * 测试 EventHandler 是否正确实现了 'accepts' 方法
   */
  @Test
  def testEventHandlerAcceptsLogic(): Unit = {
    val validEvent = RequirePermissionServiceEvent(new ObjectHolder[PermissionService])
    val invalidEvent = new OtherMockEvent()

    assertTrue(hookedEventHandler.accepts(validEvent),
      "EventHandler 应接受 RequirePermissionServiceEvent")

    assertFalse(hookedEventHandler.accepts(invalidEvent),
      "EventHandler 不应接受其他类型的事件")
  }

  /**
   * 测试链式逻辑: InnerService 接受，OldService 拒绝
   * 预期: InnerService 被调用
   */
  @Test
  def testChainingLogic_InnerServiceAccepts(): Unit = {
    // 1. 准备:
    // InnerService (构造函数中的) 接受用户并授予权限
    mockInnerService.acceptsUser = true
    mockInnerService.permissionResult = true
    // OldService (已存在的) 不接受用户
    mockOldService.acceptsUser = false
    mockOldService.permissionResult = false // (不应被调用)

    // 2. 模拟事件: 将 'mockOldService' 放入 holder
    val holder = new ObjectHolder[PermissionService]()
    holder.set(mockOldService)
    val event = RequirePermissionServiceEvent(holder)

    // 3. 执行: 处理事件，创建链式服务
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取: 获取新的链式服务
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(MockUser), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 6. 验证 checkPermission() 链 (InnerService 优先)
    val ops = List(DataOperationType.Map)
    assertTrue(chainedService.checkPermission(MockUser, "data", ops), "链式 checkPermission() 应返回 true (来自 InnerService)")

    // 验证调用
    assertTrue(mockInnerService.checkPermissionCalled, "InnerService.checkPermission 应被调用")
    assertEquals(MockUser, mockInnerService.userChecked, "InnerService 检查了错误的用户")
    assertEquals("data", mockInnerService.dataFrameChecked, "InnerService 检查了错误的 dataFrameName")
    assertEquals(ops, mockInnerService.opsChecked, "InnerService 检查了错误的 opList")

    assertFalse(mockOldService.checkPermissionCalled, "OldService.checkPermission 不应被调用")
  }

  /**
   * 测试链式逻辑: InnerService 拒绝，OldService 接受
   * 预期: OldService 被调用
   */
  @Test
  def testChainingLogic_OldServiceAccepts(): Unit = {
    // 1. 准备:
    // InnerService (构造函数中的) 不接受用户
    mockInnerService.acceptsUser = false
    // OldService (已存在的) 接受用户并授予权限
    mockOldService.acceptsUser = true
    mockOldService.permissionResult = true

    // 2. 模拟事件
    val holder = new ObjectHolder[PermissionService]()
    holder.set(mockOldService)
    val event = RequirePermissionServiceEvent(holder)

    // 3. 执行
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(MockUser), "链式 accepts() 应返回 true (因为 OldService 接受)")

    // 6. 验证 checkPermission() 链 (Inner 失败, Old 成功)
    val ops = List(DataOperationType.Filter)
    assertTrue(chainedService.checkPermission(MockUser, "data2", ops), "链式 checkPermission() 应返回 true (来自 OldService)")

    // 验证调用
    assertFalse(mockInnerService.checkPermissionCalled, "InnerService.checkPermission 不应被调用")

    assertTrue(mockOldService.checkPermissionCalled, "OldService.checkPermission 应被调用")
    assertEquals(MockUser, mockOldService.userChecked, "OldService 检查了错误的用户")
    assertEquals("data2", mockOldService.dataFrameChecked, "OldService 检查了错误的 dataFrameName")
    assertEquals(ops, mockOldService.opsChecked, "OldService 检查了错误的 opList")
  }

  /**
   * 测试链式逻辑: 两个服务都拒绝
   * 预期: 返回 false，无人被调用
   */
  @Test
  def testChainingLogic_NoServiceAccepts(): Unit = {
    // 1. 准备: 两个服务都不接受
    mockInnerService.acceptsUser = false
    mockOldService.acceptsUser = false

    // 2. 模拟事件
    val holder = new ObjectHolder[PermissionService]()
    holder.set(mockOldService)
    val event = RequirePermissionServiceEvent(holder)

    // 3. 执行
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链
    assertFalse(chainedService.accepts(MockUser), "链式 accepts() 应返回 false (因为两者都拒绝)")

    // 6. 验证 checkPermission() 链 (默认返回 false)
    assertFalse(chainedService.checkPermission(MockUser, "data", List()), "链式 checkPermission() 应返回 false")

    // 验证调用
    assertFalse(mockInnerService.checkPermissionCalled, "InnerService.checkPermission 不应被调用")
    assertFalse(mockOldService.checkPermissionCalled, "OldService.checkPermission 不应被调用")
  }

  /**
   * 测试当 Holder 中 'old' 为 null 时的链式逻辑
   * 预期: 只有 InnerService 工作
   */
  @Test
  def testChainingLogic_HolderInitiallyEmpty(): Unit = {
    // 1. 准备:
    mockInnerService.acceptsUser = true
    mockInnerService.permissionResult = true

    // 2. 模拟事件: Holder 为空 (old = null)
    val holder = new ObjectHolder[PermissionService]()
    val event = RequirePermissionServiceEvent(holder)

    // 3. 执行
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(MockUser), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 6. 验证 checkPermission() 链 (InnerService 优先)
    val ops = List(DataOperationType.Select)
    assertTrue(chainedService.checkPermission(MockUser, "data", ops), "链式 checkPermission() 应返回 true (来自 InnerService)")

    // Verify calls
    assertTrue(mockInnerService.checkPermissionCalled, "InnerService.checkPermission 应被调用 (即使 old 为 null)")
    assertEquals(ops, mockInnerService.opsChecked, "InnerService 检查了错误的 opList")
  }
}


/**
 * 模拟一个 Anchor，用于捕获被 hook 的 EventHandler
 * (与 CatalogServiceModuleTest.scala 中的 MockAnchor 相同)
 */
class MockAnchor extends Anchor {
  var hookedHandler: EventHandler = null

  override def hook(service: EventHandler): Unit = {
    this.hookedHandler = service
  }

  // 提供一个空实现以满足 trait
  override def hook(service: link.rdcn.server.EventSource): Unit = {}

}

/**
 * 模拟一个 ServerContext
 * (与 CatalogServiceModuleTest.scala 中的 MockServerContext 相同)
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
 * 模拟 PermissionService，用于跟踪调用
 */
class MockPermissionService(name: String) extends PermissionService {
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

