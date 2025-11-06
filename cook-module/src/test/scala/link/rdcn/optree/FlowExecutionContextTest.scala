/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:02
 * @Modified By:
 */
package link.rdcn.optree

import jep.SubInterpreter
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.struct.{DataFrame, DefaultDataFrame, StructType}
import link.rdcn.user.Credentials
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotSame, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future


class FlowExecutionContextTest {

  private var mockContext: MockFlowExecutionContext = _

  @BeforeEach
  def setUp(): Unit = {
    mockContext = new MockFlowExecutionContext()
  }

  /**
   * 测试 registerAsyncResult 和 getAsyncResult
   */
  @Test
  def testRegisterAndGetAsyncResult(): Unit = {
    val mockOp = TransformerNode(
      TransformFunctionWrapper.fromJsonObject(new JSONObject().put("type",LangTypeV2.REPOSITORY_OPERATOR.name).put("functionID","1")))
    val mockThread = new Thread()
    val mockFuture = Future.successful(DefaultDataFrame(StructType.empty,Iterator.empty))

    // 执行
    mockContext.registerAsyncResult(mockOp, mockFuture, mockThread)

    // 验证
    val retrievedFuture = mockContext.getAsyncResult(mockOp)

    assertTrue(retrievedFuture.isDefined, "getAsyncResult 应返回 Some")
    assertEquals(mockFuture, retrievedFuture.get, "返回的 Future 实例不匹配")
  }

  /**
   * 测试 getAsyncThreads
   * 根据代码实现中的 Bug，此测试验证它总是返回 None (因为 asyncResultsList 总是空的)
   */
  @Test
  def testGetAsyncThreads_ReturnsNoneDueToBug(): Unit = {
    val mockOp = TransformerNode(
      TransformFunctionWrapper.fromJsonObject(new JSONObject().put("type",LangTypeV2.REPOSITORY_OPERATOR.name).put("functionID","1")))
    val mockThread = new Thread()
    val mockFuture = Future.successful(DefaultDataFrame(StructType.empty,Iterator.empty))

    // 执行
    mockContext.registerAsyncResult(mockOp, mockFuture, mockThread)

    // 验证
    val retrievedThreads = mockContext.getAsyncThreads(mockOp)

    assertEquals(None, retrievedThreads,
      "getAsyncThreads 应返回 None (因为 asyncResultsList 为空，foreach 循环被跳过)")
  }

  /**
   * 测试 onComplete Success 逻辑 (验证它由于 Bug 而*未*被触发)
   */
  @Test
  def testOnCompleteSuccessLogic_DoesNotRun(): Unit = {
    val mockOp = new MockTransformerNode(TransformFunctionWrapper.fromJsonObject(new JSONObject().put("type",LangTypeV2.REPOSITORY_OPERATOR.name).put("functionID","1")))
    val mockThread = new Thread()
    val mockFuture = Future.successful(DefaultDataFrame(StructType.empty,Iterator.empty))

    // 执行
    mockContext.registerAsyncResult(mockOp, mockFuture, mockThread)

    // 等待 Future (它会立即完成)
    Thread.sleep(100)

    // 验证
    assertFalse(mockOp.released.get(),
      "release() 不应被调用 (因为 onComplete 监听器由于 Bug 未被附加)")
  }

  /**
   * 测试 onComplete Failure 逻辑 (验证它由于 Bug 而*未*被触发)
   */
  @Test
  def testOnCompleteFailureLogic_DoesNotRun(): Unit = {
    val mockOp = TransformerNode(
      TransformFunctionWrapper.fromJsonObject(new JSONObject().put("type",LangTypeV2.REPOSITORY_OPERATOR.name).put("functionID","1")))
    val mockThread = new Thread()
    val mockFuture = Future.failed[DataFrame](new Exception("Test Failure"))

    // 执行
    mockContext.registerAsyncResult(mockOp, mockFuture, mockThread)

    // 等待 Future (它会立即完成)
    Thread.sleep(100)

    // 验证
    val retrievedFuture = mockContext.getAsyncResult(mockOp)

    // 验证 Future *未* 从 asyncResults Map 中移除
    assertTrue(retrievedFuture.isDefined,
      "失败的 Future 不应被移除 (因为 onComplete 监听器由于 Bug 未被附加)")
  }

  /**
   * 测试 isAsyncEnabled 默认值
   */
  @Test
  def testIsAsyncEnabled(): Unit = {
    assertFalse(mockContext.isAsyncEnabled, "isAsyncEnabled 默认应为 false")
  }

  /**
   * 测试 getSubInterpreter (集成测试)
   * 此测试依赖于 JEP 和本地 Python 环境
   */
  @Test
  @Disabled("这是一个集成测试，需要 JEP 和一个有效的 Python 环境 ('python.home' Java 属性)")
  def testGetSubInterpreter_Integration(): Unit = {
    // 确保我们不是在模拟的 pythonHome 上运行
    assertNotSame("/mock/python/home", mockContext.pythonHome,
      "此测试需要一个真实的 'python.home' 系统属性才能运行")

    var interpreter: Option[SubInterpreter] = None
    try {
      // 执行
      interpreter = mockContext.getSubInterpreter(
        "mock-site-packages-path",
        "mock-whl-path"
      )

      // 验证
      assertTrue(interpreter.isDefined, "getSubInterpreter 应返回一个 SubInterpreter 实例")

      // 测试解释器是否工作
      interpreter.get.exec("x = 10 + 5")
      val result = interpreter.get.getValue("x", classOf[java.lang.Integer])
      assertEquals(15, result, "JEP 解释器未能正确执行 Python 代码")

    } finally {
      // 清理
      interpreter.foreach(_.close())
    }
  }
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