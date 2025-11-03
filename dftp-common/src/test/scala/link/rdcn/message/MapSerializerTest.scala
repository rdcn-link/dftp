/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/3 17:32
 * @Modified By:
 */
package link.rdcn.message

import com.fasterxml.jackson.core.JsonProcessingException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}

class MapSerializerTest {

  /**
   * 测试核心功能：
   * 1. 编码 (encodeMap)
   * 2. 解码 (decodeMap)
   * 3. 验证往返(round-trip)的数据完整性
   */
  @Test
  def testEncodeDecodeRoundTrip(): Unit = {
    // 准备一个包含所有支持类型的复杂 Map
    val originalMap: Map[String, Any] = Map(
      "stringValue" -> "Hello Gemini",
      "intValue" -> 123,
      "doubleValue" -> 99.9,
      "booleanValue" -> true,
      "nullValue" -> null,
      "nestedMap" -> Map("subKey" -> "subValue"),
      "listValue" -> List(1, "a", false, 10.5)
    )

    // 编码
    val encodedBytes = MapSerializer.encodeMap(originalMap)
    assertNotNull(encodedBytes, "编码后的字节数组不应为 null")
    assertTrue(encodedBytes.length > 0, "编码后的字节数组不应为空")

    // 解码
    val decodedMap = MapSerializer.decodeMap(encodedBytes)
    assertNotNull(decodedMap, "解码后的 Map 不应为 null")

    // 验证整个 Map 的相等性
    // Jackson 会将 List/Map 反序列化为 Java 集合，
    // 需要比较值，而不是严格的 Scala 集合类型。
    assertEquals(originalMap("stringValue"), decodedMap("stringValue"), "String 类型的值不匹配")
    assertEquals(originalMap("intValue"), decodedMap("intValue"), "Int 类型的值不匹配")
    assertEquals(originalMap("doubleValue"), decodedMap("doubleValue"), "Double 类型的值不匹配")
    assertEquals(originalMap("booleanValue"), decodedMap("booleanValue"), "Boolean 类型的值不匹配")
    assertEquals(originalMap("nullValue"), decodedMap("nullValue"), "null 值不匹配")

    // Jackson 会将嵌套的 Map 反序列化为 java.util.Map
    val expectedNestedMap = originalMap("nestedMap").asInstanceOf[Map[String, Any]]
    val actualNestedMap = decodedMap("nestedMap").asInstanceOf[Map[String, Any]]
    assertEquals(expectedNestedMap("subKey"), actualNestedMap("subKey"), "嵌套 Map 的值不匹配")

    // Jackson 会将嵌套的 List 反序列化为 java.util.List
    val expectedList = originalMap("listValue").asInstanceOf[List[Any]]
    val actualList = decodedMap("listValue").asInstanceOf[Seq[Any]]
    assertEquals(expectedList(0), actualList(0), "嵌套 List [0] 的值不匹配")
    assertEquals(expectedList(1), actualList(1), "嵌套 List [1] 的值不匹配")
    assertEquals(expectedList(2), actualList(2), "嵌套 List [2] 的值不匹配")
    assertEquals(expectedList(3), actualList(3), "嵌套 List [3] 的值不匹配")
  }

  /**
   * 测试边缘情况：空 Map
   */
  @Test
  def testEmptyMap(): Unit = {
    val emptyMap = Map.empty[String, Any]

    // 编码
    val encodedBytes = MapSerializer.encodeMap(emptyMap)

    // 遵守 assertEquals(expected, actual, message) 规范
    assertEquals("{}", new String(encodedBytes, "UTF-8"), "空 Map 编码后应为 '{}'")

    // 解码
    val decodedMap = MapSerializer.decodeMap(encodedBytes)
    assertNotNull(decodedMap, "解码后的空 Map 不应为 null")
    assertTrue(decodedMap.isEmpty, "解码后的 Map 应该是空的")
  }

  /**
   * 测试错误情况：解码无效的 JSON 字节
   */
  @Test
  def testDecodeInvalidBytes(): Unit = {
    val invalidBytes = "This is not JSON".getBytes("UTF-8")

    // 验证是否抛出了预期的 Jackson 异常
    val exception = assertThrows(classOf[JsonProcessingException], () => {
      MapSerializer.decodeMap(invalidBytes)
      ()
    }, "解码无效的 JSON 字符串应抛出 JsonProcessingException")

    assertNotNull(exception.getMessage, "异常消息不应为 null")
  }

  /**
   * 测试错误情况：解码空字节数组
   */
  @Test
  def testDecodeEmptyBytes(): Unit = {
    val emptyBytes = Array.empty[Byte]

    // 验证是否抛出异常（Jackson 无法将空输入解析为 Map）
    val exception = assertThrows(classOf[Exception], () => {
      MapSerializer.decodeMap(emptyBytes)
      ()
    }, "解码空字节数组应抛出异常")

    assertNotNull(exception.getMessage, "异常消息不应为 null")
  }
}