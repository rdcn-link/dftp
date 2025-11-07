/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:06
 * @Modified By:
 */
package link.rdcn.optree.fifo

import link.rdcn.struct.StructType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.condition.{DisabledOnOs, OS}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Random


class BinaryFilePipeTest {

  @TempDir
  var tempDir: File = _

  private var testFile: File = _
  private var pipe: BinaryFilePipe = _

  @BeforeEach
  def setUp(): Unit = {
    // 为每个测试创建一个唯一的临时文件和 BinaryFilePipe 实例
    testFile = new File(tempDir, s"test_pipe_${Random.nextLong()}.bin")
    pipe = new BinaryFilePipe(testFile)
  }

  /**
   * 测试核心的 write 和 read 往返功能
   */
  @Test
  def testWriteAndReadRoundTrip(): Unit = {
    val dataChunk1 = Array[Byte](1, 2, 3, 4, 5)
    val dataChunk2 = Array[Byte](6, 7, 8)
    val dataChunk3 = Array[Byte](9, 10, 11, 12, 13, 14, 15)
    val allData = dataChunk1 ++ dataChunk2 ++ dataChunk3

    val dataIterator = Seq(dataChunk1, dataChunk2, dataChunk3).iterator

    // 测试 Write
    pipe.write(dataIterator)


    assertTrue(testFile.exists(), "文件应在写入后存在")
    assertEquals(allData.length.toLong, testFile.length(), "写入的文件长度与原始数据总长度不匹配")

    // 测试 Read (使用一个不均匀的 chunk size 来测试边界)
    val readIterator = pipe.read(chunkSize = 7)
    val readChunks = readIterator.toList

    val readCombined = readChunks.flatten.toArray.sorted

    // 验证读回的数据
    // 注意数据可能顺序不同
    assertArrayEquals(allData, readCombined, "读回的组合数据与原始数据不匹配")

    // 验证分块是否正确 (15 字节 / 7 字节块 = 3 块)
    assertEquals(3, readChunks.size, "应读回 3 个数据块")
    assertEquals(7, readChunks(0).length, "第一个数据块应为 7 字节")
    assertEquals(7, readChunks(1).length, "第二个数据块应为 7 字节")
    assertEquals(1, readChunks(2).length, "第三个（最后一个）数据块应为 1 字节")
  }

  /**
   * 测试 dataFrame() 方法
   */
  @Test
  def testDataFrameConversion(): Unit = {
    val dataChunk1 = Array[Byte](10, 20)
    val dataChunk2 = Array[Byte](30, 40, 50)
    val data = Seq(dataChunk1, dataChunk2)

    pipe.write(data.iterator)

    // 执行
    val df = pipe.dataFrame()

    // 验证 Schema

    assertEquals(StructType.binaryStructType, df.schema, "DataFrame 的 Schema 不匹配")

    // 验证数据
    val rows = df.collect()
    assertEquals(1, rows.length, "DataFrame 的行数不匹配")

    assertArrayEquals(dataChunk1 ++ dataChunk2, rows(0)._1.asInstanceOf[Array[Byte]], "第一行的数据不匹配")
  }

  /**
   * 测试 fromExistFile 方法
   */
  @Test
  def testFromExistFile(): Unit = {
    // 准备一个源文件
    val sourceFile = new File(tempDir, "source.bin")
    val sourceData = Array[Byte](1, 5, 10, 20, 30, 100)
    Files.write(sourceFile.toPath, sourceData)

    // 执行 fromExistFile (注意：这是异步的)
    pipe.fromExistFile(sourceFile)

    // 等待异步线程完成
    // (注意：在真实的 CI/CD 中，应使用更健壮的机制，但 Thread.sleep 适用于此测试)
    Thread.sleep(1000)

    // 验证 pipe 文件中的内容
    val destinationData = Files.readAllBytes(testFile.toPath)


    assertArrayEquals(sourceData, destinationData, "fromExistFile 未能正确复制文件内容")
  }

  /**
   * 测试伴生对象中的 createEmptyFile 工厂方法
   */
  @Test
  @DisabledOnOs(value = Array(OS.WINDOWS), disabledReason = "mkfifo 是一个 Unix/Linux 命令，在 Windows 上不可用")
  def testCreateEmptyFileFactories(): Unit = {
    // 测试 (file: File) 重载
    val file1 = new File(tempDir, "empty1.bin")
    val pipe1 = BinaryFilePipe.createEmptyFile(file1)


    assertNotNull(pipe1, "Pipe (file) 不应为 null")
    assertTrue(file1.exists(), "文件 (file) 应被创建")
    assertEquals(0L, file1.length(), "文件 (file) 长度应为 0")

    // 测试 (path: String) 重载
    val file2Path = Paths.get(tempDir.getAbsolutePath, "empty2.bin").toString
    val pipe2 = BinaryFilePipe.createEmptyFile(file2Path)
    val file2 = new File(file2Path)


    assertNotNull(pipe2, "Pipe (path) 不应为 null")
    assertTrue(file2.exists(), "文件 (path) 应被创建")
    assertEquals(0L, file2.length(), "文件 (path) 长度应为 0")
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
    val dataChunk = Array[Byte](1, 2, 3)
    pipe.write(Iterator(dataChunk))

    val readIterator = pipe.read()

    assertTrue(readIterator.hasNext, "迭代器应有数据")
    assertNotNull(readIterator.next(), "第一次调用 next() 应成功")

    assertFalse(readIterator.hasNext, "迭代器在数据耗尽后 hasNext 应为 false")

    // 验证在数据耗尽后再次调用 next() 是否抛出异常
    assertThrows(classOf[NoSuchElementException], () => {
      readIterator.next()
      ()
    }, "在耗尽的迭代器上调用 next() 应抛出 NoSuchElementException")
  }
}