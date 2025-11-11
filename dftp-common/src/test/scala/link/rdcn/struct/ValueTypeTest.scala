/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.struct


import link.rdcn.struct.ValueType._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class ValueTypeTest {

  // --- DFRef 测试 ---

  @Test
  def testDFRef(): Unit = {
    val url = "dftp://example.com/data"
    val ref = DFRef(url)

    assertEquals(url, ref.value, "DFRef.value 应返回 url")
    assertEquals(RefType, ref.valueType, "DFRef.valueType 应返回 RefType")
  }

  // --- ValueType object (case objects) 测试 ---

  @Test
  def testValueType_NamesAndToString(): Unit = {
    assertEquals("Int", IntType.name, "IntType.name 不匹配")
    assertEquals("Long", LongType.name, "LongType.name 不匹配")
    assertEquals("Float", FloatType.name, "FloatType.name 不匹配")
    assertEquals("Double", DoubleType.name, "DoubleType.name 不匹配")
    assertEquals("String", StringType.name, "StringType.name 不匹配")
    assertEquals("Boolean", BooleanType.name, "BooleanType.name 不匹配")
    assertEquals("Binary", BinaryType.name, "BinaryType.name 不匹配")
    assertEquals("REF", RefType.name, "RefType.name 不匹配")
    assertEquals("Blob", BlobType.name, "BlobType.name 不匹配")
    assertEquals("Null", NullType.name, "NullType.name 不匹配")

    // toString 默认使用 name
    assertEquals("String", StringType.toString, "StringType.toString 不匹配")
  }

  // --- ValueType object (API) 测试 ---

  @Test
  def testValueType_ValuesList(): Unit = {
    val values = ValueType.values

    assertEquals(8, values.length, "ValueType.values 序列应包含 8 个元素")

    // 验证包含的类型
    assertTrue(values.contains(IntType), "values 序列应包含 IntType")
    assertTrue(values.contains(NullType), "values 序列应包含 NullType")

    // 验证(故意)省略的类型
    assertFalse(values.contains(RefType), "values 序列不应包含 RefType")
    assertFalse(values.contains(BlobType), "values 序列不应包含 BlobType")
  }

  @Test
  def testValueType_FromName_HappyPath(): Unit = {
    assertEquals(Some(IntType), ValueType.fromName("Int"), "fromName('Int') 失败")
    assertEquals(Some(StringType), ValueType.fromName("String"), "fromName('String') 失败")
    assertEquals(Some(NullType), ValueType.fromName("Null"), "fromName('Null') 失败")
  }

  @Test
  def testValueType_FromName_SynonymsAndCase(): Unit = {
    // 测试同义词
    assertEquals(Some(IntType), ValueType.fromName("integer"), "fromName('integer') 失败")
    assertEquals(Some(BooleanType), ValueType.fromName("bool"), "fromName('bool') 失败")
    assertEquals(Some(BinaryType), ValueType.fromName("bytes"), "fromName('bytes') 失败")

    // 测试大小写不敏感
    assertEquals(Some(IntType), ValueType.fromName("int"), "fromName('int') 失败")
    assertEquals(Some(StringType), ValueType.fromName("STRING"), "fromName('STRING') 失败")
    assertEquals(Some(BooleanType), ValueType.fromName("BoOlEaN"), "fromName('BoOlEaN') 失败")
  }

  @Test
  def testValueType_FromName_Failure(): Unit = {
    // 根据代码实现，fromName 在失败时不返回 None，而是抛出 Exception

    // 1. 测试未知的类型
    val exUnknown = assertThrows(classOf[Exception], () => {
      ValueType.fromName("UnknownType")
      ()
    }, "fromName('UnknownType') 应抛出异常")

    assertTrue(exUnknown.getMessage.contains("does not exist unknowntype"), "异常消息不正确")

    // 2. 测试未在fromName中定义的类型 (例如 REF)
    val exRef = assertThrows(classOf[Exception], () => {
      ValueType.fromName("REF")
      ()
    }, "fromName('REF') 应抛出异常")

    assertTrue(exRef.getMessage.contains("does not exist ref"), "异常消息不正确")
  }

  @Test
  def testValueType_IsNumeric(): Unit = {
    assertTrue(ValueType.isNumeric(IntType), "isNumeric(IntType) 应为 true")
    assertTrue(ValueType.isNumeric(LongType), "isNumeric(LongType) 应为 true")
    assertTrue(ValueType.isNumeric(FloatType), "isNumeric(FloatType) 应为 true")
    assertTrue(ValueType.isNumeric(DoubleType), "isNumeric(DoubleType) 应为 true")

    assertFalse(ValueType.isNumeric(StringType), "isNumeric(StringType) 应为 false")
    assertFalse(ValueType.isNumeric(BooleanType), "isNumeric(BooleanType) 应为 false")
    assertFalse(ValueType.isNumeric(BinaryType), "isNumeric(BinaryType) 应为 false")
    assertFalse(ValueType.isNumeric(NullType), "isNumeric(NullType) 应为 false")
    assertFalse(ValueType.isNumeric(RefType), "isNumeric(RefType) 应为 false")
    assertFalse(ValueType.isNumeric(BlobType), "isNumeric(BlobType) 应为 false")
  }

  // --- ValueTypeHelper object 测试 ---

  @Test
  def testHelper_GetAllTypes(): Unit = {
    assertEquals(ValueType.values, ValueTypeHelper.getAllTypes, "getAllTypes() 应返回 ValueType.values")
  }

  @Test
  def testHelper_GetSpecificTypes(): Unit = {
    assertEquals(IntType, ValueTypeHelper.getIntType, "getIntType 失败")
    assertEquals(LongType, ValueTypeHelper.getLongType, "getLongType 失败")
    assertEquals(FloatType, ValueTypeHelper.getFloatType, "getFloatType 失败")
    assertEquals(DoubleType, ValueTypeHelper.getDoubleType, "getDoubleType 失败")
    assertEquals(StringType, ValueTypeHelper.getStringType, "getStringType 失败")
    assertEquals(BooleanType, ValueTypeHelper.getBooleanType, "getBooleanType 失败")
    assertEquals(BinaryType, ValueTypeHelper.getBinaryType, "getBinaryType 失败")
    assertEquals(NullType, ValueTypeHelper.getNullType, "getNullType 失败")
  }

  @Test
  def testHelper_IsNumeric(): Unit = {
    assertTrue(ValueTypeHelper.isNumeric(IntType), "Helper.isNumeric(IntType) 应为 true")
    assertFalse(ValueTypeHelper.isNumeric(StringType), "Helper.isNumeric(StringType) 应为 false")
  }

  @Test
  def testHelper_FromName_HappyPath(): Unit = {
    assertEquals(IntType, ValueTypeHelper.fromName("Int"), "Helper.fromName('Int') 失败")
    assertEquals(IntType, ValueTypeHelper.fromName("integer"), "Helper.fromName('integer') 失败")
    assertEquals(StringType, ValueTypeHelper.fromName("STRING"), "Helper.fromName('STRING') 失败")
  }

  @Test
  def testHelper_FromName_Failure(): Unit = {
    // ValueTypeHelper.fromName 会捕获 ValueType.fromName 的 Exception
    // 并重新抛出一个 IllegalArgumentException。

    val ex = assertThrows(classOf[Exception], () => {
      ValueTypeHelper.fromName("UnknownType")
      ()
    }, "Helper.fromName('UnknownType') 应抛出异常 (来自 ValueType.fromName)")

    assertTrue(ex.getMessage.contains("does not exist unknowntype"), "异常消息不正确")
  }
}