/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:06
 * @Modified By:
 */
package link.rdcn.user

import link.rdcn.dacp.user.DataOperationType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * 针对 DataOperationType object 的单元测试
 */
class DataOperationTypeTest {

  /**
   * 测试 fromString 方法是否能通过精确匹配的字符串找到对象
   */
  @Test
  def testFromString_ExactMatch(): Unit = {
    val expected = Some(DataOperationType.Map)
    val actual = DataOperationType.fromString("Map")
    assertEquals(expected, actual, "fromString('Map') 应返回 Some(DataOperationType.Map)")
  }

  /**
   * 测试 fromString 方法是否能忽略大小写
   */
  @Test
  def testFromString_IgnoreCase(): Unit = {
    // 测试全小写
    val expectedLower = Some(DataOperationType.Filter)
    val actualLower = DataOperationType.fromString("filter")
    assertEquals(expectedLower, actualLower, "fromString('filter') 应不区分大小写并返回 Some(DataOperationType.Filter)")

    // 测试全大写
    val expectedUpper = Some(DataOperationType.Join)
    val actualUpper = DataOperationType.fromString("JOIN")
    assertEquals(expectedUpper, actualUpper, "fromString('JOIN') 应不区分大小写并返回 Some(DataOperationType.Join)")

    // 测试混合大小写
    val expectedMixed = Some(DataOperationType.Select)
    val actualMixed = DataOperationType.fromString("sElEcT")
    assertEquals(expectedMixed, actualMixed, "fromString('sElEcT') 应不区分大小写并返回 Some(DataOperationType.Select)")
  }

  /**
   * 测试 fromString 方法在找不到匹配项时的行为
   */
  @Test
  def testFromString_NotFound(): Unit = {
    val expected = None
    val actual = DataOperationType.fromString("NonExistentType")
    assertEquals(expected, actual, "fromString('NonExistentType') 应返回 None")
  }

  /**
   * 测试 fromString 方法在输入为空字符串时的行为
   */
  @Test
  def testFromString_EmptyString(): Unit = {
    val expected = None
    val actual = DataOperationType.fromString("")
    assertEquals(expected, actual, "fromString('') 应返回 None")
  }

  /**
   * 测试 values 序列是否包含所有定义的 case object
   */
  @Test
  def testValuesList_ContainsAllTypes(): Unit = {
    val values = DataOperationType.values

    // 验证数量
    assertEquals(7, values.length, "values 序列应包含 7 个元素")

    // 验证所有成员是否存在
    assertTrue(values.contains(DataOperationType.Map), "values 序列应包含 Map")
    assertTrue(values.contains(DataOperationType.Filter), "values 序列应包含 Filter")
    assertTrue(values.contains(DataOperationType.Select), "values 序列应包含 Select")
    assertTrue(values.contains(DataOperationType.Reduce), "values 序列应包含 Reduce")
    assertTrue(values.contains(DataOperationType.Join), "values 序列应包含 Join")
    assertTrue(values.contains(DataOperationType.GroupBy), "values 序列应包含 GroupBy")
    assertTrue(values.contains(DataOperationType.Sort), "values 序列应包含 Sort")
  }
}