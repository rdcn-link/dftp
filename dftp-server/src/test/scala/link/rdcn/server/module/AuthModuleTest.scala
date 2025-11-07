/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 18:27
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.user.AuthenticationService
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}


class AuthModuleTest {

  private var mockOldService: MockAuthenticationService = _
  private var mockInnerService: MockAuthenticationService = _
  private var moduleToTest: AuthModule = _
  private var mockAnchor: MockAnchor = _
  private var hookedEventHandler: EventHandler = _

  @BeforeEach
  def setUp(): Unit = {
    // 1. 准备: 创建两个不同的模拟服务
    mockOldService = new MockAuthenticationService("OldService")
    mockInnerService = new MockAuthenticationService("InnerService")


    // 2. 准备: 创建被测模块，将 'innerService' 传入构造函数
    moduleToTest = new AuthModule(mockInnerService)
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
    val validEvent = new RequireAuthenticatorEvent(new ObjectHolder[AuthenticationService])
    val invalidEvent = new OtherMockEvent()

    assertTrue(hookedEventHandler.accepts(validEvent),
      "EventHandler 应接受 RequireAuthenticatorEvent")

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
    // InnerService (构造函数中的) 接受凭证
    mockInnerService.acceptsCreds = true
    // OldService (已存在的) 不接受凭证
    mockOldService.acceptsCreds = false

    // 2. 模拟事件: 将 'mockOldService' 放入 holder
    val holder = new ObjectHolder[AuthenticationService]()
    holder.set(mockOldService)
    val event = new RequireAuthenticatorEvent(holder)

    // 3. 执行: 处理事件，创建链式服务
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取: 获取新的链式服务
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(MockCredentials), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 6. 验证 authenticate() 链 (InnerService 优先)
    val user = chainedService.authenticate(MockCredentials)

    assertEquals(mockInnerService.userToReturn, user, "链式 authenticate() 应返回 InnerService 的 User")

    // 验证调用
    assertTrue(mockInnerService.authenticateCalled, "InnerService.authenticate 应被调用")
    assertEquals(MockCredentials, mockInnerService.credsChecked, "InnerService 检查了错误的凭证")

    assertFalse(mockOldService.authenticateCalled, "OldService.authenticate 不应被调用")
  }

  /**
   * 测试链式逻辑: InnerService 拒绝，OldService 接受
   * 预期: OldService 被调用
   */
  @Test
  def testChainingLogic_OldServiceAccepts(): Unit = {
    // 1. 准备:
    // InnerService (构造函数中的) 不接受凭证
    mockInnerService.acceptsCreds = false
    // OldService (已存在的) 接受凭证
    mockOldService.acceptsCreds = true
    mockOldService.userToReturn = MockUser // 确保返回的是 OldService 的用户

    // 2. 模拟事件
    val holder = new ObjectHolder[AuthenticationService]()
    holder.set(mockOldService)
    val event = new RequireAuthenticatorEvent(holder)

    // 3. 执行
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(MockCredentials), "链式 accepts() 应返回 true (因为 OldService 接受)")

    // 6. 验证 authenticate() 链 (Inner 失败, Old 成功)
    val user = chainedService.authenticate(MockCredentials)

    assertEquals(mockOldService.userToReturn, user, "链式 authenticate() 应返回 OldService 的 User")

    // 验证调用
    assertFalse(mockInnerService.authenticateCalled, "InnerService.authenticate 不应被调用")

    assertTrue(mockOldService.authenticateCalled, "OldService.authenticate 应被调用")
    assertEquals(MockCredentials, mockOldService.credsChecked, "OldService 检查了错误的凭证")
  }

  /**
   * 测试链式逻辑: 两个服务都拒绝
   * 预期: 抛出异常，无人被调用
   */
  @Test
  def testChainingLogic_NoServiceAccepts(): Unit = {
    // 1. 准备: 两个服务都不接受
    mockInnerService.acceptsCreds = false
    mockOldService.acceptsCreds = false

    // 2. 模拟事件
    val holder = new ObjectHolder[AuthenticationService]()
    holder.set(mockOldService)
    val event = new RequireAuthenticatorEvent(holder)

    // 3. 执行
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链
    assertFalse(chainedService.accepts(MockCredentials), "链式 accepts() 应返回 false (因为两者都拒绝)")

    // 6. 验证 authenticate() 链 (抛出异常)
    val ex = assertThrows(classOf[Exception], () => {
      chainedService.authenticate(MockCredentials)
      ()
    }, "链式 authenticate() 在两者都拒绝时应抛出异常")

    assertTrue(ex.getMessage.contains("unrecognized credentials"), "异常消息应指明凭证未被识别")

    // 验证调用
    assertFalse(mockInnerService.authenticateCalled, "InnerService.authenticate 不应被调用")
    assertFalse(mockOldService.authenticateCalled, "OldService.authenticate 不应被调用")
  }

  /**
   * 测试当 Holder 中 'old' 为 null 时的链式逻辑
   * 预期: 只有 InnerService 工作
   */
  @Test
  def testChainingLogic_HolderInitiallyEmpty(): Unit = {
    // 1. 准备:
    mockInnerService.acceptsCreds = true

    // 2. 模拟事件: Holder 为空 (old = null)
    val holder = new ObjectHolder[AuthenticationService]()
    val event = new RequireAuthenticatorEvent(holder)

    // 3. 执行
    hookedEventHandler.doHandleEvent(event)

    // 4. 提取
    val chainedService = holder.invoke(run = s => s, onNull = null)
    assertNotNull(chainedService, "Holder 不应为空")

    // 5. 验证 accepts() 链 (OR 逻辑)
    assertTrue(chainedService.accepts(MockCredentials), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 6. 验证 authenticate() 链 (InnerService 优先)
    val user = chainedService.authenticate(MockCredentials)

    assertEquals(mockInnerService.userToReturn, user, "链式 authenticate() 应返回 InnerService 的 User")

    // 验证调用
    assertTrue(mockInnerService.authenticateCalled, "InnerService.authenticate 应被调用 (即使 old 为 null)")
    assertEquals(MockCredentials, mockInnerService.credsChecked, "InnerService 检查了错误的凭证")
  }
}
