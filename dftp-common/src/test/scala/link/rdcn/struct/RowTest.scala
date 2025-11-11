/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:53
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util
import scala.collection.JavaConverters._

class RowTest {

  // 准备一个包含各种类型的标准 Row 用于多个测试
  val testRow = new Row(Seq(1, "hello", true, null, Array(10.0, 20.0)))

  /**
   * 测试基本的构造函数和访问器 (get, length, isEmpty)
   */
  @Test
  def testCoreAccessors(): Unit = {
    assertEquals(5, testRow.length, "Row.length 应返回 5")
    assertEquals(1, testRow.get(0), "Row.get(0) 应返回 1")
    assertEquals("hello", testRow.get(1), "Row.get(1) 应返回 'hello'")
    assertFalse(testRow.isEmpty, "testRow.isEmpty 应返回 false")
    assertTrue(Row.empty.isEmpty, "Row.empty.isEmpty 应返回 true")

    // 测试越界
    val ex = assertThrows(classOf[IndexOutOfBoundsException], () => {
      testRow.get(99)
      ()
    }, "get(99) 应抛出 IndexOutOfBoundsException")
  }

  /**
   * 测试安全的 getOpt 访问器
   */
  @Test
  def testGetOpt(): Unit = {
    assertEquals(Some(1), testRow.getOpt(0), "getOpt(0) 应返回 Some(1)")
    assertEquals(Some(null), testRow.getOpt(3), "getOpt(3) 应返回 Some(null)")
    assertEquals(None, testRow.getOpt(99), "getOpt(99) (越界) 应返回 None")
    assertEquals(None, testRow.getOpt(-1), "getOpt(-1) (越界) 应返回 None")
  }

  /**
   * 测试强类型转换 getAs[T]
   */
  @Test
  def testGetAs(): Unit = {
    assertEquals(1, testRow.getAs[Int](0), "getAs[Int](0) 应返回 1")
    assertEquals("hello", testRow.getAs[String](1), "getAs[String](1) 应返回 'hello'")
    assertTrue(testRow.getAs[Boolean](2), "getAs[Boolean](2) 应返回 true")

    // 测试 getAs[T] 返回 null
    assertNull(testRow.getAs[String](3), "getAs[String](3) (值为null) 应返回 null")
  }

  /**
   * 测试 getAs[T] 的失败情况
   */
  @Test
  def testGetAs_FailureCases(): Unit = {
    // 测试索引越界
    val exIndex = assertThrows(classOf[NoSuchElementException], () => {
      testRow.getAs[String](99)
      ()
    }, "getAs[String](99) 应抛出 NoSuchElementException")

  }

  /**
   * 测试类元组 (tuple-like) 访问器 _1, _2 等
   */
  @Test
  def testTupleAccessors(): Unit = {
    val row = Row(10, "a", false)
    assertEquals(10, row._1, "row._1 应返回第一个元素")
    assertEquals("a", row._2, "row._2 应返回第二个元素")
    assertEquals(false, row._3, "row._3 应返回第三个元素")
  }

  /**
   * 测试 toSeq, toArray, iterator
   */
  @Test
  def testConversionMethods(): Unit = {
    assertEquals(testRow.values, testRow.toSeq, "toSeq 应返回原始 Seq")

    // 验证 toArray
    val arr = testRow.toArray
    assertEquals(5, arr.length, "toArray 返回的数组长度不匹配")
    assertEquals(1, arr(0), "toArray 数组的第一个元素不匹配")
    assertTrue(arr(4).isInstanceOf[Array[Double]], "toArray 数组的第五个元素类型不匹配")

    // 验证 iterator
    val iter = testRow.iterator
    assertTrue(iter.isInstanceOf[Iterator[Any]], "iterator 应返回一个 Iterator")
    assertEquals(1, iter.next(), "Iterator 的第一个元素不匹配")
    assertEquals("hello", iter.next(), "Iterator 的第二个元素不匹配")
  }

  /**
   * 测试 prepend 和 append (不可变性)
   */
  @Test
  def testPrependAndAppend(): Unit = {
    // 1. 测试 prepend
    val prependedRow = testRow.prepend("newHead")

    assertFalse(testRow.eq(prependedRow), "prepend 应返回一个*新*的 Row 实例")
    assertEquals(6, prependedRow.length, "prepend 后新 Row 的长度不正确")
    assertEquals("newHead", prependedRow.get(0), "prepend 后新 Row 的第一个元素不正确")
    assertEquals(1, prependedRow.get(1), "prepend 后新 Row 的第二个元素 (原第一个元素) 不正确")

    // 2. 测试 append
    val appendedRow = testRow.append("newTail")

    assertFalse(testRow.eq(appendedRow), "append 应返回一个*新*的 Row 实例")
    assertEquals(6, appendedRow.length, "append 后新 Row 的长度不正确")
    assertEquals(1, appendedRow.get(0), "append 后新 Row 的第一个元素不正确")
    assertEquals("newTail", appendedRow.get(5), "append 后新 Row 的最后一个元素不正确")
  }

  /**
   * 测试 toString 方法对 null 和 Array 的格式化
   */
  @Test
  def testToStringFormatting(): Unit = {
    val expectedString = "Row(1, hello, true, null, Array(10.0, 20.0))"
    assertEquals(expectedString, testRow.toString, "toString() 的格式化不正确")

    val simpleRow = Row("a", null)
    assertEquals("Row(a, null)", simpleRow.toString, "toString() 对 null 的格式化不正确")
  }

  /**
   * 测试 toJsonString 和 toJsonObject
   */
  @Test
  def testJsonConversion(): Unit = {
    // 准备一个 StructType
    val schema = StructType.empty
      .add("id", ValueType.IntType)
      .add("name", ValueType.StringType)
      .add("active", ValueType.BooleanType)

    val row = Row(99, "Alice", true)

    // 1. 测试 toJsonObject
    val jsonObj = row.toJsonObject(schema)
    assertEquals(99, jsonObj.get("id"), "toJsonObject 的 'id' 字段不匹配")
    assertEquals("Alice", jsonObj.get("name"), "toJsonObject 的 'name' 字段不匹配")
    assertEquals(true, jsonObj.get("active"), "toJsonObject 的 'active' 字段不匹配")

    // 2. 测试 toJsonString
    assertEquals(jsonObj.toString, row.toJsonString(schema), "toJsonString 应返回与 toJsonObject.toString 相同的结果")

    // 3. 测试 StructType 和 Row 列数不匹配 (zip 行为)
    val shortSchema = StructType.empty.add("id", ValueType.IntType)
    val shortJson = row.toJsonObject(shortSchema)

    assertEquals(99, shortJson.get("id"), "当 Schema 较短时，'id' 字段应匹配")
    assertFalse(shortJson.has("name"), "当 Schema 较短时，不应包含 'name' 字段")
  }

  // --- 测试 object Row 工厂方法 ---

  /**
   * 测试 Row.apply (varargs)
   */
  @Test
  def testFactory_Apply(): Unit = {
    val row = Row(1, "a", true)
    assertEquals(Seq(1, "a", true), row.toSeq, "Row.apply(varargs) 构造失败")
  }

  /**
   * 测试 Row.fromSeq
   */
  @Test
  def testFactory_FromSeq(): Unit = {
    val seq = Seq(1, "a", true)
    val row = Row.fromSeq(seq)
    assertEquals(seq, row.toSeq, "Row.fromSeq 构造失败")
  }

  /**
   * 测试 Row.fromArray
   */
  @Test
  def testFactory_FromArray(): Unit = {
    val arr = Array[Any](1, "a", true)
    val row = Row.fromArray(arr)
    assertEquals(arr.toSeq, row.toSeq, "Row.fromArray 构造失败")
  }

  /**
   * 测试 Row.fromTuple
   */
  @Test
  def testFactory_FromTuple(): Unit = {
    val tuple = (1, "a", true)
    val row = Row.fromTuple(tuple)
    assertEquals(Seq(1, "a", true), row.toSeq, "Row.fromTuple 构造失败")
  }

  /**
   * 测试 Row.fromJavaList
   */
  @Test
  def testFactory_FromJavaList(): Unit = {
    val javaList = new util.ArrayList[Object]()
    javaList.add(1.asInstanceOf[Object])
    javaList.add("a".asInstanceOf[Object])
    javaList.add(true.asInstanceOf[Object])

    val row = Row.fromJavaList(javaList)
    assertEquals(javaList.asScala.toSeq, row.toSeq, "Row.fromJavaList 构造失败")
  }

  /**
   * 测试 Row.fromJsonString
   */
  @Test
  def testFactory_FromJsonString(): Unit = {
    val jsonStr = """{"id": 123, "name": "Bob", "active": false}"""
    val row = Row.fromJsonString(jsonStr)

    // 注意: JSONObject 不保证键的顺序
    val expectedValues = Set[Any](123, "Bob", false)
    assertEquals(3, row.length, "fromJsonString 解析的 Row 长度不匹配")
    assertEquals(expectedValues, row.toSeq.toSet, "fromJsonString 解析的 Row 内容不匹配")
  }

  /**
   * 测试 Row.zipWithIndex
   */
  @Test
  def testHelper_ZipWithIndex(): Unit = {
    val row = Row("a", "b", "c")
    val expected = Seq((0, "a"), (1, "b"), (2, "c"))
    val actual = Row.zipWithIndex(row)
    assertEquals(expected, actual, "Row.zipWithIndex 返回的结果不正确")
  }
}