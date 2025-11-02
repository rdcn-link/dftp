/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/30 17:58
 * @Modified By:
 */
package link.rdcn.server

import junit.framework.Assert.assertNotNull
import link.rdcn.server.DftpServerStartTest.{mockAnchor, tempConfFile, testCreds}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipalWithCredentials}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.lang.reflect.Method
import java.util.Properties

object DftpServerStartTest {
  private var authModule: AuthModule = null
  private var mockAnchor: MockAnchor = null
  private val mockContext = new MockServerContext()
  private val testCreds = Credentials.ANONYMOUS
  private var tempConfFile: File = null
  private val confContent =
    """
      |dftp.host.position=localhost
      |dftp.host.port=8080
      |dftp.chinese.test=\u4F60\u597D
      |""".stripMargin // 你好

  /**
 * Setup: 创建一个临时的 dftp.conf 文件用于测试
 */
  @BeforeAll
  def setUp(): Unit = {
    authModule = new AuthModule()
    mockAnchor = new MockAnchor()
    // 执行 init，这会触发 hook
    authModule.init(mockAnchor, mockContext)
    try {
      tempConfFile = File.createTempFile("dftp_test", ".conf")
      val writer = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(tempConfFile), "UTF-8"))
      try {
        writer.print(confContent)
      } finally {
        writer.close()
      }
    } catch {
      case e: Exception => fail("Setup 失败: " + e.getMessage)
    }
  }

  /**
   * TearDown: 删除临时文件
   */
  @AfterAll
  def tearDown(): Unit = {
    if (tempConfFile != null && tempConfFile.exists()) {
      tempConfFile.delete()
    }
  }

}

class DftpServerStartTest {
  /**
   * 测试 init 方法是否成功 hook 了服务
   */
  @Test
  def testInitHook(): Unit = {
    assertNotNull("AuthService 应该被 hook 到 Anchor", mockAnchor.hookedService)
  }

  /**
   * 测试被 hook 的 AuthenticationService 的 accepts 逻辑
   */
  @Test
  def testAuthServiceAccepts(): Unit = {
    val service = mockAnchor.hookedService
    assertNotNull("Service 不应为 null", service)

    assertTrue(service.accepts(testCreds), "accepts 应该总是返回 true")
  }

  /**
   * 测试被 hook 的 AuthenticationService 的 authenticate 逻辑
   */
  @Test
  def testAuthServiceAuthenticate(): Unit = {
    val service = mockAnchor.hookedService
    assertNotNull("Service 不应为 null", service)

    val expectedPrincipal = UserPrincipalWithCredentials(testCreds)
    val actualPrincipal = service.authenticate(testCreds)

    assertEquals(expectedPrincipal, actualPrincipal,
      "authenticate 应该返回 UserPrincipalWithCredentials")
  }

  /**
   * 测试私有方法 loadProperties (通过反射)
   */
  @Test
  def testLoadProperties(): Unit = {
    try {
      // 1. 通过反射获取私有方法
      val method: Method = DftpServerStart.getClass.getDeclaredMethod(
        "loadProperties", classOf[String])

      // 2. 设置为可访问
      method.setAccessible(true)

      // 3. 调用方法
      val props = method.invoke(DftpServerStart, tempConfFile.getAbsolutePath)
        .asInstanceOf[Properties]

      // 4. 断言
      assertNotNull("Properties 对象不应为 null", props)

      // 遵守 [2025-09-26] 规范：(expected, actual, message)
      assertEquals("localhost", props.getProperty("dftp.host.position"),
        "未能正确加载 host")
      assertEquals("8080", props.getProperty("dftp.host.port"),
        "未能正确加载 port")
      assertEquals("你好", props.getProperty("dftp.chinese.test"),
        "未能正确加载 UTF-8 字符")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        fail("通过反射调用 loadProperties 失败: " + e.getMessage)
    }
  }

  /**
   * 测试 main 方法在没有参数时是否按预期失败
   * (我们使用 try-catch 来精确捕获 sys.error 抛出的异常并验证其消息)
   */
  @Test
  def testMainNoArgsFailure(): Unit = {
    try {
      DftpServerStart.main(Array[String]())
      // 如果没有抛出异常，测试失败
      fail("main 方法在没有参数时应该抛出 sys.error")
    } catch {
      case e: RuntimeException => // sys.error 抛出 RuntimeException
        // 遵守 [2025-09-26] 规范：(expected, actual, message)
        assertEquals("need set Dftp Home", e.getMessage,
          "异常消息不匹配")
      case t: Throwable =>
        fail(s"捕获了意外的 Throwable (非 RuntimeException): $t")
    }
  }
}

trait DftpModule {
  def init(anchor: Anchor, serverContext: ServerContext): Unit
  def destroy(): Unit
}
trait Anchor {
  def hook(service: AuthenticationService): Unit
}
trait ServerContext
class DftpServer(config: DftpServerConfig) {
  def addModule(module: DftpModule): DftpServer = this
  def startBlocking(): Unit = {}
}
case class DftpServerConfig(host: String, port: Int, home: Option[String])

// --- 假设的用户/凭证类 (用于编译) ---
trait Credentials
trait UserPrincipal
object MockCredentials extends Credentials

// --- 模拟 Anchor (用于测试) ---
class MockAnchor extends Anchor {
  var hookedService: AuthenticationService = null
  override def hook(service: AuthenticationService): Unit = {
    this.hookedService = service
  }
}

// --- 模拟 ServerContext (用于测试) ---
class MockServerContext extends ServerContext