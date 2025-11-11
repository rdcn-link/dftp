package link.rdcn.util

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.IOException
import scala.collection.mutable.ListBuffer

/**
 * ResourceUtils 的 JUnit 5 测试类
 * (使用 Scala 编写)
 */
class ResourceUtilsTest {

  // 用于跟踪 close() 调用顺序的列表
  private val closeOrderTracker = ListBuffer[String]()

  /**
   * 一个可追踪的 AutoCloseable 桩实现，用于测试。
   *
   * @param id 资源的标识符
   * @param throwOnClose 如果为 true，在调用 close() 时抛出 IOException
   */
  class MockResource(val id: String, val throwOnClose: Boolean = false) extends AutoCloseable {
    var isClosed = false
    var actionCount = 0

    def performAction(): Unit = {
      if (isClosed) throw new IllegalStateException(s"Resource $id already closed")
      actionCount += 1
    }

    override def close(): Unit = {
      if (!isClosed) {
        isClosed = true
        closeOrderTracker.append(id)
        if (throwOnClose) {
          throw new IOException(s"Failed to close $id")
        }
      }
    }
  }

  @BeforeEach
  def setUp(): Unit = {
    // 在每次测试前清空跟踪器
    closeOrderTracker.clear()
  }

  // --- Tests for using ---

  @Test
  def testUsing_Success(): Unit = {
    val res = new MockResource("res1")
    var blockExecuted = false

    val result = ResourceUtils.using(res) { r =>
      r.performAction()
      blockExecuted = true
      "success" // 返回一个值
    }

    assertTrue(blockExecuted, "代码块应被执行")
    assertEquals(1, res.actionCount, "资源上的方法应被调用")
    assertEquals("success", result, "应返回代码块的执行结果")
    assertTrue(res.isClosed, "资源在成功执行后应被关闭")
    assertEquals(List("res1"), closeOrderTracker.toList, "close() 方法应被调用")
  }

  @Test
  def testUsing_ExceptionInBlock(): Unit = {
    val res = new MockResource("res-fail")
    val expectedExceptionMsg = "Error in block"

    val ex = assertThrows(classOf[RuntimeException], () => {
      ResourceUtils.using(res) { r =>
        r.performAction()
        throw new RuntimeException(expectedExceptionMsg)
      }
      () // 遵守 [2025-09-26] 规范
    }, "代码块中的异常应被抛出")

    assertEquals(expectedExceptionMsg, ex.getMessage, "应抛出原始异常")
    assertEquals(1, res.actionCount, "资源在抛出异常前应被使用")
    assertTrue(res.isClosed, "即使代码块失败，资源也应被关闭")
    assertEquals(List("res-fail"), closeOrderTracker.toList, "close() 方法应被调用")
  }

  @Test
  def testUsing_NullResource(): Unit = {

    // 修正：f(resource) 会立即执行 f(null)，如果 f 尝试访问 r，会抛出 NPE
    // 'using' 的主要作用是确保 'finally' 块安全

    // 测试1：'finally' 块的安全性
    assertThrows(classOf[NullPointerException], () => {
      ResourceUtils.using(null.asInstanceOf[MockResource]) { r =>
        r.performAction() // 这里会抛出 NPE
      }
      ()
    }, "当 f(null) 被调用时，访问 r 会抛出 NPE")

    // 测试2：如果 f 块不访问 null 资源，'finally' 块应安全处理 null
    ResourceUtils.using(null.asInstanceOf[MockResource]) { r =>
      // 不对 r 做任何操作
    }
    // 如果没有抛出 NPE，说明 'finally' 块安全地处理了 'null'

    assertTrue(closeOrderTracker.isEmpty, "没有资源被创建，所以 close 不应被调用")
  }

  // --- Tests for usingAll ---

  @Test
  def testUsingAll_SuccessAndReverseOrder(): Unit = {
    val res1 = new MockResource("res1")
    val res2 = new MockResource("res2")
    val res3 = new MockResource("res3")
    var blockExecuted = false

    ResourceUtils.usingAll(res1, res2, res3) {
      blockExecuted = true
    }

    assertTrue(blockExecuted, "代码块应被执行")

    // 验证所有资源都已关闭
    assertTrue(res1.isClosed, "res1 应被关闭")
    assertTrue(res2.isClosed, "res2 应被关闭")
    assertTrue(res3.isClosed, "res3 应被关闭")

    // 关键：验证关闭顺序是相反的
    val expectedOrder = List("res3", "res2", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "资源应按声明的相反顺序关闭")
  }

  @Test
  def testUsingAll_ExceptionInBlock(): Unit = {
    val resA = new MockResource("resA")
    val resB = new MockResource("resB")
    val expectedExceptionMsg = "All block failed"

    val ex = assertThrows(classOf[RuntimeException], () => {
      ResourceUtils.usingAll(resA, resB) {
        throw new RuntimeException(expectedExceptionMsg)
      }
      ()
    }, "代码块中的异常应被抛出")

    assertEquals(expectedExceptionMsg, ex.getMessage, "应抛出原始异常")

    // 即使代码块失败，所有资源仍应按相反顺序关闭
    assertTrue(resA.isClosed, "resA 应被关闭")
    assertTrue(resB.isClosed, "resB 应被关闭")

    val expectedOrder = List("resB", "resA")
    assertEquals(expectedOrder, closeOrderTracker.toList, "资源应按相反顺序关闭")
  }

  @Test
  def testUsingAll_ExceptionInClose(): Unit = {
    val res1 = new MockResource("res1")
    val res2_fails = new MockResource("res2_fails", throwOnClose = true)
    val res3 = new MockResource("res3")

    // 'usingAll' 应捕获 res2_fails 抛出的异常，并继续关闭 res1
    // 不应有异常被抛出
    ResourceUtils.usingAll(res1, res2_fails, res3) {
      // 块成功执行
    }

    // 验证所有资源都 *尝试* 关闭（即使 res2 失败了）
    assertTrue(res1.isClosed, "res1 应被关闭")
    assertTrue(res2_fails.isClosed, "res2_fails (失败的) 应被标记为已关闭")
    assertTrue(res3.isClosed, "res3 应被关闭")

    // 验证关闭顺序 (res3, res2_fails, res1)
    val expectedOrder = List("res3", "res2_fails", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "即使有失败，也应按相反顺序尝试关闭所有资源")
  }

  @Test
  def testUsingAll_ExceptionInBlockAndClose(): Unit = {
    val res1 = new MockResource("res1")
    val res2_fails = new MockResource("res2_fails", throwOnClose = true)
    val expectedExceptionMsg = "Block failed first"

    // 当块和 finally 都抛出异常时，块的异常应被保留并抛出

    val ex = assertThrows(classOf[RuntimeException], () => {
    ResourceUtils.usingAll(res1, res2_fails) {
    throw new RuntimeException(expectedExceptionMsg)
  }
    ()
  }, "代码块中的异常应被抛出")

    // 验证块的异常被抛出
    assertEquals(expectedExceptionMsg, ex.getMessage, "应抛出原始的块异常")

    // 验证 close 仍被尝试
    assertTrue(res1.isClosed, "res1 应被关闭")
    assertTrue(res2_fails.isClosed, "res2_fails 应被关闭")

    val expectedOrder = List("res2_fails", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "close 仍然按相反顺序执行")
  }

  @Test
  def testUsingAll_WithNulls(): Unit = {
    val res1 = new MockResource("res1")
    val res3 = new MockResource("res3")

    // 不应抛出 NullPointerException
    ResourceUtils.usingAll(res1, null, res3) {
      // 块成功执行
    }

    assertTrue(res1.isClosed, "res1 应被关闭")
    assertTrue(res3.isClosed, "res3 应被关闭")

    // 验证关闭顺序 (res3, res1)
    val expectedOrder = List("res3", "res1")
    assertEquals(expectedOrder, closeOrderTracker.toList, "Null 资源应被安全跳过")
  }
}