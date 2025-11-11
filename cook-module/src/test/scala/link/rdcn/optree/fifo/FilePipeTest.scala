/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:12
 * @Modified By:
 */
package link.rdcn.optree.fifo

import link.rdcn.dacp.optree.fifo.FilePipe
import link.rdcn.optree.fifo.FilePipeTest.{pipe, testFile}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.condition.{DisabledOnOs, OS}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, Test}

import java.io.File
import java.nio.file.Files
import scala.util.Random

object FilePipeTest {

  @TempDir
  var tempDir: File = _

  var testFile: File = _
  var pipe: MockFilePipe = _


  @BeforeAll
  def setUp(): Unit = {
    // 为每个测试创建一个唯一的临时文件路径
    testFile = new File(tempDir, s"test_pipe_${Random.nextLong()}.fifo")
    if(testFile.exists()) testFile.delete()
    pipe = new MockFilePipe(testFile)
  }

}

class FilePipeTest {

  /**
   * 测试 create() 方法在 Unix/Linux/macOS 上的行为
   * mkfifo 在 Windows 上不可用，因此此测试在 Windows 上会被禁用
   */
  @Test
  @DisabledOnOs(value = Array(OS.WINDOWS), disabledReason = "mkfifo 是一个 Unix/Linux 命令，在 Windows 上不可用")
  def testCreate_OnUnix(): Unit = {
    assertFalse(testFile.exists(), "文件在 create() 之前不应存在")

    // 执行
    pipe.create()

    // 验证
    assertTrue(testFile.exists(), "文件在 create() 之后应存在")
    // FIFO 管道 (Named Pipe) 不是一个常规文件
    assertFalse(Files.isRegularFile(testFile.toPath), "创建的文件不应是常规文件（应为 FIFO）")
  }

  /**
   * 测试 create() 方法在文件已存在时的行为（跨平台）
   * 它不应该执行 mkfifo，也不应该抛出异常
   */
  @Test
  @DisabledOnOs(value = Array(OS.WINDOWS), disabledReason = "mkfifo 是一个 Unix/Linux 命令，在 Windows 上不可用")
  def testCreate_FileAlreadyExists(): Unit = {
    // 准备：手动创建一个*常规*文件
    testFile.createNewFile()
    val initialLength = testFile.length()

    assertTrue(testFile.exists(), "预创建的常规文件应存在")

    // 执行
    pipe.create() // 此时 if (!file.exists()) 应为 false

    // 验证
    assertTrue(testFile.exists(), "文件在 create() 后仍应存在")
    assertEquals(initialLength, testFile.length(), "文件长度不应改变")
    assertTrue(Files.isRegularFile(testFile.toPath), "文件类型不应改变（仍为常规文件）")
  }

  /**
   * 测试 delete() 方法
   */
  @Test
  def testDelete(): Unit = {
    // 准备：创建一个文件
    testFile.createNewFile()
    assertTrue(testFile.exists(), "测试文件在删除前应存在")

    // 执行删除
    pipe.delete()
    assertFalse(testFile.exists(), "测试文件在删除后不应存在")

    // 再次执行删除 (验证 Files.deleteIfExists)
    // 这一步不应抛出异常
    pipe.delete()
  }

  /**
   * 测试 path() 方法
   */
  @Test
  def testPath(): Unit = {
    val expectedPath = testFile.getAbsolutePath
    val actualPath = pipe.path

    assertEquals(expectedPath, actualPath, "path() 方法返回的绝对路径不匹配")
  }

  /**
   * 测试 dataFrame() (我们的模拟实现)
   */
  @Test
  def testDataFrame(): Unit = {
    val df = pipe.dataFrame()

    assertNotNull(df, "dataFrame() 不应返回 null")
    assertEquals("schema(mock: String)", df.schema.toString, "DataFrame schema 不匹配")
    assertEquals(1, df.collect().length, "DataFrame 应有一行数据")
  }
  @AfterEach
  def release(): Unit = {
    if(testFile!=null) testFile.delete()
  }
}

/**
 * FilePipe 是一个抽象类，我们需要一个具体的实现来进行测试。
 */
class MockFilePipe(file: File) extends FilePipe(file) {
  /**
   * 提供一个 dataFrame 的模拟实现
   */
  override def dataFrame(): DataFrame = {
    DefaultDataFrame(StructType.empty.add("mock", StringType), Seq(Row("mockData")).iterator)
  }
}
