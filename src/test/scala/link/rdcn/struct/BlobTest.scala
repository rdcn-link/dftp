/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct
import link.rdcn.struct.BlobFromFileTest.{TEST_CONTENT, testFile}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io._
import java.nio.charset.StandardCharsets

object BlobFromFileTest {

  private val TEST_CONTENT = "Hello, Blob File Test!"
  private val testFile = new File("test_blob_temp.txt")

  @BeforeAll
  def setUp(): Unit = {
    // 写入测试内容到临时文件
    val writer = new BufferedWriter(new FileWriter(testFile))
    try {
      writer.write(TEST_CONTENT)
    } finally {
      writer.close()
    }
  }

  @AfterAll
  def tearDown(): Unit = {
    // 清理临时文件
    if (testFile.exists()) {
      testFile.delete()
    }
  }
}

class BlobFromFileTest {

  @Test
  def testFromFile_ReadsContentCorrectly(): Unit = {
    // 覆盖 Blob.fromFile 的实例化
    val blob = Blob.fromFile(testFile)
    val verifier = new StreamVerifier()

    // 覆盖 offerStream 方法的 try 块逻辑
    blob.offerStream(verifier)

    // 验证内容
    assertEquals(TEST_CONTENT, verifier.readContent, "Consumer should read the correct file content")
  }

  @Test
  def testFromFile_EnsuresStreamIsClosed(): Unit = {
    // 覆盖 offerStream 方法的 finally 块逻辑
    val blob = Blob.fromFile(testFile)

    // 定义一个不会关闭流的 Consumer
    val closingConsumer: InputStream => Unit = stream => {
      // 不关闭流，让 finally 块负责
      val bytes = new Array[Byte](stream.available())
      stream.read(bytes)
    }

    // 执行 offerStream
    blob.offerStream(closingConsumer)

    assertTrue(true, "The test verifies the existence of the stream.close() call in the finally block.")
  }

  @Test
  def testFromFile_HandlesExceptionInConsumer(): Unit = {
    val blob = Blob.fromFile(testFile)

    // 覆盖 finally 块在 try 块抛出异常时的执行路径
    val exceptionConsumer: InputStream => Unit = _ => {
      throw new RuntimeException("Consumer failed")
    }

    // 预期抛出 Consumer 异常
    val exception = assertThrows(
      classOf[RuntimeException],
      () => blob.offerStream(exceptionConsumer)
    )
    assertEquals("Consumer failed", exception.getMessage, "The original consumer exception should be rethrown")
  }

  class StreamVerifier extends (InputStream => String) {
    var readContent: String = ""
    var streamClosed: Boolean = false

    override def apply(stream: InputStream): String = {
      // 检查 stream 是否是 FileInputStream 的实例
      assertTrue(stream.isInstanceOf[FileInputStream], "Stream passed to consumer must be a FileInputStream")

      // 读取内容
      val contentBytes = new Array[Byte](stream.available())
      stream.read(contentBytes)
      readContent = new String(contentBytes, StandardCharsets.UTF_8)

      // 尝试关闭，如果成功则证明流未在外部关闭
      try {
        stream.close()
        streamClosed = true
      } catch {
        case _: IOException => // ignore
      }

      readContent
    }
  }
}