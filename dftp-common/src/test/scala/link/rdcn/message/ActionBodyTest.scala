/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:49
 * @Modified By:
 */
package link.rdcn.message

import link.rdcn.server.ActionBody
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets

class ActionBodyTest {

  @Test
  def testEncodeDecodeSuccessRoundtrip(): Unit = {
    // 预期数据
    val expectedData = "Some Binary Payload".getBytes(StandardCharsets.UTF_8)
    val expectedParams: Map[String, Any] = Map(
      "user_id" -> 101,
      "query" -> "SELECT * FROM table",
      "isAdmin" -> true
    )
    val originalActionBody = ActionBody(expectedData, expectedParams)

    val encodedBytes = originalActionBody.encode()

    assertTrue(encodedBytes.length > 0, "Encoded bytes should not be empty")

    val (actualData, actualParams) = ActionBody.decode(encodedBytes)

    assertArrayEquals(expectedData, actualData, "Decoded data bytes should match original data")

    assertEquals(expectedParams.keys.toSet, actualParams.keys.toSet, "Decoded map keys should match original keys")
    assertEquals(expectedParams.get("query").get, actualParams.get("query").get, "String parameter 'query' should match")

    assertEquals(expectedParams.get("user_id").get.asInstanceOf[Int].toLong, actualParams.get("user_id").get.asInstanceOf[Number].longValue(), "Integer parameter 'user_id' should match")
    assertEquals(expectedParams.get("isAdmin").get, actualParams.get("isAdmin").get, "Boolean parameter 'isAdmin' should match")
  }

  @Test
  def testEncodeDecodeEmptyData(): Unit = {
    // 预期数据
    val expectedData = Array.emptyByteArray
    val expectedParams: Map[String, Any] = Map(
      "status" -> "ok"
    )
    val originalActionBody = ActionBody(expectedData, expectedParams)

    val encodedBytes = originalActionBody.encode()

    val (actualData, actualParams) = ActionBody.decode(encodedBytes)

    assertArrayEquals(expectedData, actualData, "Decoded data bytes should be empty")
    assertEquals(expectedParams, actualParams, "Decoded map should match original map")
  }

  @Test
  def testEncodeDecodeEmptyParams(): Unit = {
    // 预期数据
    val expectedData = "data".getBytes(StandardCharsets.UTF_8)
    val expectedParams: Map[String, Any] = Map.empty
    val originalActionBody = ActionBody(expectedData, expectedParams)

    val encodedBytes = originalActionBody.encode()

    val (actualData, actualParams) = ActionBody.decode(encodedBytes)

    assertArrayEquals(expectedData, actualData, "Decoded data bytes should match original data")
    assertEquals(expectedParams, actualParams, "Decoded map should be empty")
  }
}