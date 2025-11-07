/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:12
 * @Modified By:
 */
package link.rdcn.optree.fifo

import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{Row, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.condition.{DisabledOnOs, OS}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.Files
import scala.util.Random

class RowFilePipeTest {

  @TempDir
  var tempDir: File = _

  private var testFile: File = _
  private var pipe: RowFilePipe = _

  @BeforeEach
  def setUp(): Unit = {
    // 为每个测试创建一个唯一的临时文件和 RowFilePipe 实例
    testFile = new File(tempDir, s"test_pipe_${Random.nextLong()}.txt")
    pipe = new RowFilePipe(testFile)
  }

  /**
   * 测试核心的 write 和 read 往返功能
   */
  @Test
  def testWriteAndReadRoundTrip(): Unit = {
    val dataLines = Seq("Hello World", "This is line 2", "最后一行", "") // 包含空行
    val dataIterator = dataLines.iterator

    // 测试 Write
    pipe.write(dataIterator)

    assertTrue(testFile.exists(), "文件应在写入后存在")

    // 测试 Read
    val readIterator = pipe.read()
    val readLines = readIterator.toList
    readIterator.close()

    // 验证读回的数据
    assertEquals(dataLines, readLines, "读回的行数据与原始数据不匹配")
  }

  /**
   * 测试 dataFrame() 方法
   */
  @Test
  def testDataFrameConversion(): Unit = {
    val dataLines = Seq("line1", "line2", "line3")
    pipe.write(dataLines.iterator)

    // 执行
    val df = pipe.dataFrame()

    // 验证 Schema
    val expectedSchema = StructType.empty.add("content", StringType)
    assertEquals(expectedSchema, df.schema, "DataFrame 的 Schema 不匹配")

    // 验证数据
    val rows = df.collect()
    assertEquals(dataLines.length, rows.length, "DataFrame 的行数不匹配")

    val expectedRows = dataLines.map(line => Row.fromSeq(Seq(line)))
    assertEquals(expectedRows.toString, rows.toList.toString, "DataFrame 的内容不匹配")
  }

  /**
   * 测试 fromExistFile 方法
   * 此方法依赖于 DataUtils.getFileLines
   */
  @Test
  def testFromExistFile(): Unit = {
    // 准备一个源文件
    val sourceFile = new File(tempDir, "source.txt")
    val sourceData = Seq("source line 1", "source line 2")
    val writer = new PrintWriter(new FileWriter(sourceFile))
    try {
      sourceData.foreach(writer.println)
    } finally {
      writer.close()
    }

    // 执行 fromExistFile (它依赖 DataUtils.getFileLines)
    pipe.fromExistFile(sourceFile)

    // 验证 pipe 文件中的内容
    val readData = pipe.read().toList

    assertEquals(sourceData, readData, "fromExistFile 未能正确复制文件内容")
  }

  /**
   * 测试伴生对象中的 fromFilePath 和 fromFile 工厂方法 (Unix/Linux/macOS)
   * mkfifo 在 Windows 上不可用
   */
  @Test
  @DisabledOnOs(value = Array(OS.WINDOWS), disabledReason = "mkfifo 是一个 Unix/Linux 命令，在 Windows 上不可用")
  def testFactories_OnUnix(): Unit = {
    // 测试 fromFile(file: File)
    val file1 = new File(tempDir, "empty1.fifo")
    val pipe1 = RowFilePipe.fromFile(file1)

    assertNotNull(pipe1, "Pipe (file) 不应为 null")
    assertTrue(file1.exists(), "文件 (file) 应被创建 (mkfifo)")
    assertFalse(Files.isRegularFile(file1.toPath), "文件 (file) 不应是常规文件（应为 FIFO）")

    // 测试 fromFilePath(path: String)
    val file2Path = new File(tempDir, "empty2.fifo").getAbsolutePath
    val pipe2 = RowFilePipe.fromFilePath(file2Path)
    val file2 = new File(file2Path)

    assertNotNull(pipe2, "Pipe (path) 不应为 null")
    assertTrue(file2.exists(), "文件 (path) 应被创建 (mkfifo)")
    assertFalse(Files.isRegularFile(file2.toPath), "文件 (path) 不应是常规文件（应为 FIFO）")
  }

  /**
   * 测试工厂方法在文件已存在时的行为（跨平台）
   */
  @Test
  def testFactories_FileAlreadyExists(): Unit = {
    // 准备：手动创建一个*常规*文件
    testFile.createNewFile()
    assertTrue(testFile.exists(), "预创建的常规文件应存在")

    // 执行 (使用 fromFile)
    val pipe1 = RowFilePipe.fromFile(testFile)

    // 验证 (create() 逻辑应被跳过)
    assertTrue(testFile.exists(), "文件在 fromFile() 后仍应存在")
    assertTrue(Files.isRegularFile(testFile.toPath), "文件类型不应改变（仍为常规文件）")
  }

  /**
   * 测试边缘情况：写入和读取空数据
   */
  @Test
  def testWriteAndReadEmptyData(): Unit = {
    val emptyIterator = Iterator.empty

    // 写空数据
    pipe.write(emptyIterator)

    assertTrue(testFile.exists(), "文件应被创建，即使是空的")
    assertEquals(0L, testFile.length(), "文件长度应为 0")

    // 读空数据
    val readIterator = pipe.read()
    assertFalse(readIterator.hasNext, "读取空文件时 hasNext 应为 false")

    // 验证 dataFrame
    val df = pipe.dataFrame()
    assertEquals(0, df.collect().length, "空文件的 DataFrame 应有 0 行")
  }

  /**
   * 测试读取迭代器在完成后抛出异常
   */
  @Test
  def testReadIteratorThrowsExceptionWhenExhausted(): Unit = {
    val dataLines = Seq("one line")
    pipe.write(dataLines.iterator)

    val readIterator = pipe.read()

    assertTrue(readIterator.hasNext, "迭代器应有数据")
    assertEquals("one line", readIterator.next(), "第一次调用 next() 应返回数据")

    assertFalse(readIterator.hasNext, "迭代器在数据耗尽后 hasNext 应为 false")

    // 验证在数据耗尽后再次调用 next() 是否抛出异常
    assertThrows(classOf[NoSuchElementException], () => {
      readIterator.next()
      ()
    }, "在耗尽的迭代器上调用 next() 应抛出 NoSuchElementException")

    readIterator.close()
  }
}
