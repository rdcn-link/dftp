/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.struct.ValueType._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * 针对 StructType class 和 StructType object 的单元测试
 */
class StructTypeTest {

  // 准备一个包含各种类型的标准 StructType 用于多个测试
  val baseSchema = StructType.fromNamesAndTypes(
    "id" -> IntType,
    "name" -> StringType,
    "active" -> BooleanType
  )

  /**
   * 测试构造函数：验证重复列名
   */
  @Test
  def testConstructor_DuplicateNamesFail(): Unit = {
    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromNamesAndTypes(
        "id" -> IntType,
        "name" -> StringType,
        "id" -> BooleanType // 重复
      )
      ()
    }, "StructType 构造函数在遇到重复列名时应抛出异常")

    assertTrue(ex.getMessage.contains("存在重复列名 id"), "异常消息应指明重复的列名")
  }

  /**
   * 测试 isEmpty 和 StructType.empty
   */
  @Test
  def testIsEmptyAndEmpty(): Unit = {
    assertTrue(StructType.empty.isEmpty(), "StructType.empty.isEmpty() 应返回 true")
    assertFalse(baseSchema.isEmpty(), "baseSchema.isEmpty() 应返回 false")
  }

  /**
   * 测试 getType(columnName: String)
   */
  @Test
  def testGetType(): Unit = {
    assertEquals(Some(IntType), baseSchema.getType("id"), "getType('id') 应返回 Some(IntType)")
    assertEquals(Some(StringType), baseSchema.getType("name"), "getType('name') 应返回 Some(StringType)")
    assertEquals(None, baseSchema.getType("nonexistent"), "getType('nonexistent') 应返回 None")
  }

  /**
   * 测试 contains(columnName: String) 和 contains(colType: ValueType)
   */
  @Test
  def testContains(): Unit = {
    assertTrue(baseSchema.contains("id"), "contains('id') 应返回 true")
    assertFalse(baseSchema.contains("nonexistent"), "contains('nonexistent') 应返回 false")

    assertTrue(baseSchema.contains(IntType), "contains(IntType) 应返回 true")
    assertFalse(baseSchema.contains(FloatType), "contains(FloatType) 应返回 false")
  }

  /**
   * 测试 columnNames
   */
  @Test
  def testColumnNames(): Unit = {
    assertEquals(Seq("id", "name", "active"), baseSchema.columnNames, "columnNames 返回的 Seq 不匹配")
  }

  /**
   * 测试 indexOf(columnName: String)
   */
  @Test
  def testIndexOf(): Unit = {
    assertEquals(Some(0), baseSchema.indexOf("id"), "indexOf('id') 应返回 Some(0)")
    assertEquals(Some(1), baseSchema.indexOf("name"), "indexOf('name') 应返回 Some(1)")
    assertEquals(Some(2), baseSchema.indexOf("active"), "indexOf('active') 应返回 Some(2)")
    assertEquals(None, baseSchema.indexOf("nonexistent"), "indexOf('nonexistent') 应返回 None")
  }

  /**
   * 测试 columnAt(index: Int)
   */
  @Test
  def testColumnAt(): Unit = {
    assertEquals(Column("id", IntType, true), baseSchema.columnAt(0), "columnAt(0) 返回的 Column 不匹配")
    assertEquals(Column("name", StringType, true), baseSchema.columnAt(1), "columnAt(1) 返回的 Column 不匹配")

    // 测试越界
    val ex = assertThrows(classOf[IndexOutOfBoundsException], () => {
      baseSchema.columnAt(99)
      ()
    }, "columnAt(99) 应抛出 IndexOutOfBoundsException")
  }

  /**
   * 测试 add(name, colType, nullable)
   */
  @Test
  def testAdd(): Unit = {
    val newSchema = baseSchema.add("salary", DoubleType, nullable = false)

    // 验证不可变性
    assertEquals(3, baseSchema.columns.length, "原始 schema 不应被修改")

    // 验证新 schema
    assertEquals(4, newSchema.columns.length, "add() 后的新 schema 长度不正确")
    assertEquals(Column("salary", DoubleType, false), newSchema.columnAt(3), "add() 添加的列定义不正确")
    assertEquals(Some(3), newSchema.indexOf("salary"), "add() 后的新 schema 索引不正确")
  }

  /**
   * 测试 select(columnNames: String*)
   */
  @Test
  def testSelect(): Unit = {
    // 1. 测试成功
    val selectedSchema = baseSchema.select("name", "id")

    assertEquals(2, selectedSchema.columns.length, "select() 后的 schema 长度不正确")
    assertEquals(Column("name", StringType, true), selectedSchema.columnAt(0), "select() 后的第一列不正确")
    assertEquals(Column("id", IntType, true), selectedSchema.columnAt(1), "select() 后的第二列不正确")

    // 2. 测试失败（选择不存在的列）
    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      baseSchema.select("id", "nonexistent")
      ()
    }, "select() 选择不存在的列时应抛出异常")

    assertTrue(ex.getMessage.contains("列名 'nonexistent' 不存在"), "异常消息应指明不存在的列")
  }

  /**
   * 测试 prepend 和 append
   */
  @Test
  def testPrependAndAppend(): Unit = {
    val headCol = Column("newHead", BinaryType)
    val tailCol = Column("newTail", BinaryType)

    // 1. 测试 prepend
    val prepended = baseSchema.prepend(headCol)
    assertNotSame(baseSchema, prepended, "prepend 应返回一个新实例")
    assertEquals(4, prepended.columns.length, "prepend 后的长度不正确")
    assertEquals(headCol, prepended.columnAt(0), "prepend 后的第一列不正确")
    assertEquals("id", prepended.columnAt(1).name, "prepend 后原有的列索引不正确")

    // 2. 测试 append
    val appended = baseSchema.append(tailCol)
    assertNotSame(baseSchema, appended, "append 应返回一个新实例")
    assertEquals(4, appended.columns.length, "append 后的长度不正确")
    assertEquals(tailCol, appended.columnAt(3), "append 后的最后一列不正确")
  }

  /**
   * 测试 toString 方法
   */
  @Test
  def testToString(): Unit = {
    val expected = "schema(id: Int, name: String, active: Boolean)"
    assertEquals(expected, baseSchema.toString, "toString() 格式不匹配")

    val emptyExpected = "schema()"
    assertEquals(emptyExpected, StructType.empty.toString, "空 schema 的 toString() 格式不匹配")
  }

  // --- 测试 object StructType 工厂方法 ---

  @Test
  def testFactory_FromColumns(): Unit = {
    val cols = Seq(Column("a", IntType), Column("b", StringType))
    val schema = StructType.fromColumns(cols: _*) // 使用 varargs
    assertEquals(cols, schema.columns, "fromColumns 构造失败")
  }

  @Test
  def testFactory_FromSeq(): Unit = {
    val cols = Seq(Column("a", IntType), Column("b", StringType))
    val schema = StructType.fromSeq(cols)
    assertEquals(cols, schema.columns, "fromSeq 构造失败")
  }

  @Test
  def testFactory_FromNamesAndTypes(): Unit = {
    val expectedCols = Seq(Column("a", IntType), Column("b", StringType))
    val schema = StructType.fromNamesAndTypes("a" -> IntType, "b" -> StringType)
    assertEquals(expectedCols, schema.columns, "fromNamesAndTypes 构造失败")
  }

  @Test
  def testFactory_FromNamesAsAny(): Unit = {
    val expectedCols = Seq(Column("a", StringType), Column("b", StringType))
    val schema = StructType.fromNamesAsAny(Seq("a", "b"))
    assertEquals(expectedCols, schema.columns, "fromNamesAsAny 构造失败")
  }

  /**
   * 测试 StructType.fromString (Happy Path)
   */
  @Test
  def testFactory_FromString_HappyPath(): Unit = {
    val schemaString = "schema(id: Int, name: String, active: Boolean)"
    val schema = StructType.fromString(schemaString)

    // 比较 columns，因为 case class StructType 的 equals 默认比较这个
    assertEquals(baseSchema.columns, schema.columns, "从字符串解析的 schema 不匹配")
  }

  /**
   * 测试 StructType.fromString (边缘情况 - 空)
   */
  @Test
  def testFactory_FromString_EmptyCases(): Unit = {
    assertEquals(StructType.empty.columns, StructType.fromString("schema()").columns, "fromString('schema()') 应返回 empty")
    assertEquals(StructType.empty.columns, StructType.fromString("schema(   )").columns, "fromString('schema( )') (带空格) 应返回 empty")
    assertEquals(StructType.empty.columns, StructType.fromString("").columns, "fromString('') 应返回 empty")
  }

  /**
   * 测试 StructType.fromString (失败情况)
   */
  @Test
  def testFactory_FromString_FailureCases(): Unit = {
    // 1. 格式错误 - 缺少前缀
    assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromString("id: IntType)")
      ()
    }, "fromString() 缺少 'schema(' 前缀时应抛出异常")

    // 2. 格式错误 - 缺少后缀
    assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromString("schema(id: IntType")
      ()
    }, "fromString() 缺少 ')' 后缀时应抛出异常")

    // 3. 列格式错误
    val exCol = assertThrows(classOf[IllegalArgumentException], () => {
      StructType.fromString("schema(id IntType)") // 缺少 ':'
      ()
    }, "fromString() 列定义格式错误时应抛出异常")
    assertTrue(exCol.getMessage.contains("无效的列定义格式: 'id IntType'"), "异常消息应指明错误的列定义")

    // 4. 类型错误 (依赖 ValueTypeHelper)
    val exType = assertThrows(classOf[Exception], () => {
      StructType.fromString("schema(id: UnknownType)")
      ()
    }, "fromString() 遇到未知类型时应抛出异常")
  }

  /**
   * 测试标准 Schema (blobStreamStructType, binaryStructType)
   */
  @Test
  def testStandardSchemas(): Unit = {
    // 1. blobStreamStructType
    val blobSchema = StructType.blobStreamStructType
    assertEquals(1, blobSchema.columns.length, "blobStreamStructType 应有 1 列")
    assertEquals(Column("content", BinaryType), blobSchema.columnAt(0), "blobStreamStructType 的列定义不正确")

    // 2. binaryStructType
    val binarySchema = StructType.binaryStructType
    assertEquals(7, binarySchema.columns.length, "binaryStructType 应有 7 列")
    assertEquals(Column("name", StringType), binarySchema.columnAt(0), "binaryStructType 的 'name' 列不正确")
    assertEquals(Column("File", BlobType), binarySchema.columnAt(6), "binaryStructType 的 'File' 列不正确")
  }
}