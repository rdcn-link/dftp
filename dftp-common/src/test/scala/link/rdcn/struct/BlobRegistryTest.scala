package struct

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:50
 * @Modified By:
 */
import link.rdcn.struct.{Blob, BlobRegistry}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterAll, BeforeEach, Test}
import struct.BlobRegistryTest.{TEST_DATA, mockBlob, testBlobId}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

object BlobRegistryTest {

  private val TEST_DATA = "Test data for stream".getBytes(StandardCharsets.UTF_8)
  private val mockBlob = new Blob {
    override def offerStream[T](consume: InputStream => T): T = {
      val stream = new ByteArrayInputStream(TEST_DATA)
      consume(stream)
    }
  }
  private var testBlobId: String = _


  @AfterAll
  def tearDown(): Unit = {
    // 确保每次测试后清理注册表，防止泄露
    BlobRegistry.cleanUp()
  }

}

class BlobRegistryTest {

  @BeforeEach
  def setUp(): Unit = {
    // 确保每次测试开始时注册一个 blob 并获取其 ID
    testBlobId = BlobRegistry.register(mockBlob)
  }


  @Test
  def testRegisterAndGetBlobSuccess(): Unit = {
    // 覆盖 register 和 getBlob 的成功路径
    val retrievedBlob = BlobRegistry.getBlob(testBlobId)

    assertTrue(retrievedBlob.isDefined, "Should successfully retrieve the registered Blob")
    assertTrue(retrievedBlob.get eq mockBlob, "Retrieved Blob should be the same instance (reference equality)")
  }

  @Test
  def testGetBlobNotFound(): Unit = {
    // 覆盖 getBlob 的失败路径
    val nonExistentId = UUID.randomUUID().toString
    val retrievedBlob = BlobRegistry.getBlob(nonExistentId)

    assertTrue(retrievedBlob.isEmpty, "Should return None for non-existent ID")
  }

  @Test
  def testGetStreamSuccess(): Unit = {
    // 定义一个用于消费 InputStream 的函数
    val consumer: InputStream => String = stream => {
      val bytes = new Array[Byte](stream.available())
      stream.read(bytes)
      new String(bytes, StandardCharsets.UTF_8)
    }
    val result = BlobRegistry.getStream(testBlobId)(consumer)

    assertTrue(result.isDefined, "Should return Some(result) when Blob is found")
    assertEquals(new String(TEST_DATA, StandardCharsets.UTF_8), result.get, "Result from consumer must match original data")
  }

  @Test
  def testGetStreamNotFound(): Unit = {
    // 覆盖 getStream 的 Blob not found 路径
    val nonExistentId = UUID.randomUUID().toString

    // 定义一个用于消费的函数，它不应该被调用
    val consumer: InputStream => String = _ => {
      fail("Consumer should not be called when Blob is not found")
      "Should not be reached"
    }

    // 预期返回 None
    val result = BlobRegistry.getStream(nonExistentId)(consumer)

    assertTrue(result.isEmpty, "Should return None when Blob is not found")
  }

  @Test
  def testCleanUp(): Unit = {
    // 验证 cleanUp()
    BlobRegistry.cleanUp() // 覆盖 cleanUp()
    val retrievedBlob = BlobRegistry.getBlob(testBlobId)

    assertTrue(retrievedBlob.isEmpty, "Registry should be empty after cleanUp()")
  }
}