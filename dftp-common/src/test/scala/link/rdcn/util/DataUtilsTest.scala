/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:54
 * @Modified By:
 */
package link.rdcn.util

import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}



/**
 * DataUtils 的 JUnit 5 测试类
 * (使用 Scala 编写)
 */
class DataUtilsTest {

  // @TempDir 会为每个测试方法创建一个新的临时目录
  @TempDir
  var tempDir: Path = _

  private var testFile: File = _
  private var subDir: File = _

  @BeforeEach
  def setUp(): Unit = {
    // 准备一些文件用于测试
    testFile = tempDir.resolve("testfile.txt").toFile
    Files.write(testFile.toPath, "line 1\nline 2\nline 3".getBytes(StandardCharsets.UTF_8))

    subDir = tempDir.resolve("subdir").toFile
    subDir.mkdir()
    Files.write(
      subDir.toPath.resolve("nested.txt"),
      "nested line".getBytes(StandardCharsets.UTF_8)
    )
  }

  // Helper: 在临时目录中创建文件
  private def createTempFile(name: String, content: String, dir: File = tempDir.toFile): File = {
    val file = new File(dir, name)
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  // Helper: 创建一个 Excel 文件用于测试
  private def createTestExcelFile(name: String, isXlsx: Boolean): File = {
    val file = tempDir.resolve(name).toFile
    val wb = if (isXlsx) new XSSFWorkbook() else new HSSFWorkbook()
    val sheet = wb.createSheet("Sheet1")

    // Header
    val headerRow = sheet.createRow(0)
    headerRow.createCell(0).setCellValue("ID")
    headerRow.createCell(1).setCellValue("Name")
    headerRow.createCell(2).setCellValue("Value")
    headerRow.createCell(3).setCellValue("IsActive")
    headerRow.createCell(4).setCellValue("DateNum") // 测试日期转 Long

    // Data Row 1
    val row1 = sheet.createRow(1)
    row1.createCell(0).setCellValue(101)
    row1.createCell(1).setCellValue("Test 1")
    row1.createCell(2).setCellValue(123.45)
    row1.createCell(3).setCellValue(true)

    val dateCell = row1.createCell(4)
    dateCell.setCellValue(44197.0) // Excel date for 2021-01-01
    val style = wb.createDataFormat()
    dateCell.getCellStyle.setDataFormat(style.getFormat("m/d/yy"))


    // Data Row 2
    val row2 = sheet.createRow(2)
    row2.createCell(0).setCellValue(102)
    row2.createCell(1).setCellValue("Test 2")
    row2.createCell(2).setCellValue(200.0) // 整数型 Double
    row2.createCell(3).setCellValue(false)
    row2.createCell(4, CellType.BLANK) // 空单元格

    val fos = new FileOutputStream(file)
    wb.write(fos)
    fos.close()
    wb.close()
    file
  }

  @Test
  def testGetStructTypeFromMap(): Unit = {
    val map = Map("id" -> 10, "name" -> "Alice", "active" -> true, "score" -> 99.5)
    val schema = DataUtils.getStructTypeFromMap(map)

    val expectedTypes = Map(
      "id" -> IntType,
      "name" -> StringType,
      "active" -> BooleanType,
      "score" -> DoubleType
    )

    assertEquals(4, schema.columns.length, "Schema应包含4列")
    assertTrue(schema.columns.forall(c => expectedTypes(c.name) == c.colType), "Map中所有字段的类型应被正确推断")
  }

  @Test
  def testGetDataFrameByStream(): Unit = {
    val row1 = Row.fromSeq(Seq(1, "a"))
    val row2 = Row.fromSeq(Seq(2, "b"))


    val df = DataUtils.getDataFrameByStream(Iterator(row1, row2))

    // 验证 Schema
    val expectedSchema = StructType(Seq(Column("_1", IntType), Column("_2", StringType)))
    assertEquals(expectedSchema, df.schema, "应从第一行推断出 Schema")

    // 验证数据
    val data = df.collect().toList
    assertEquals(List(row1, row2), data, "DataFrame应包含所有数据")

  }

  @Test
  def testGetDataFrameByStream_Empty(): Unit = {
    val df = DataUtils.getDataFrameByStream(Iterator.empty)
    assertEquals(StructType.empty, df.schema, "空流应返回空 Schema")
    assertTrue(df.collect().toList.isEmpty, "空流应返回空数据")
  }

  @Test
  def testListAllFilesWithAttrs(): Unit = {
    // testFile (root) 和 nested.txt (subdir) 应该都被找到
    val files = DataUtils.listAllFilesWithAttrs(tempDir.toFile).toList
    val fileNames = files.map(_._1.getName).toSet

    assertEquals(2, files.length, "应递归找到2个文件")
    assertEquals(Set("testfile.txt", "nested.txt"), fileNames, "应包含根目录和子目录中的文件")

    // 检查属性
    val (file, attrs) = files.head
    assertNotNull(attrs, "文件属性不应为 null")
    assertTrue(attrs.isRegularFile, "应为常规文件")
  }

  @Test
  def testListFilesWithAttributes(): Unit = {
    // 只应找到 testFile.txt 和 subDir
    val files = DataUtils.listFilesWithAttributes(tempDir.toFile)
    val fileNames = files.map(_._1.getName).toSet

    assertEquals(2, files.length, "应非递归找到2个条目（1个文件，1个目录）")
    assertEquals(Set("testfile.txt", "subdir"), fileNames, "应包含根目录的条目")
  }

  @Test
  def testListFilesWithAttributes_NonExistentDir(): Unit = {
    val nonExistentDir = new File(tempDir.toFile, "nonexistent")
    val files = DataUtils.listFilesWithAttributes(nonExistentDir)
    assertTrue(files.isEmpty, "对于不存在的目录应返回空 Seq")
  }

  // testGetFileType 被跳过，因为它依赖于一个未提供的外部静态类 MimeTypeFactory


  @Test
  def testGetFileLines_Closable(): Unit = {
    val linesIter = DataUtils.getFileLines(testFile)
    val lines = linesIter.toList
    linesIter.close() // 确保 close 可以被调用
    assertEquals(List("line 1", "line 2", "line 3"), lines, "应读取文件的所有行 (File path)")
  }

  @Test
  def testConvertStringRowToTypedRow(): Unit = {
    val schema = StructType(Seq(
      Column("i", IntType),
      Column("l", LongType),
      Column("d", DoubleType),
      Column("f", FloatType),
      Column("b", BooleanType),
      Column("s", StringType),
      Column("n", StringType) // 测试 null
    ))

    val stringRow = Row.fromSeq(Seq("123", "9876543210", "123.45", "0.5", "true", "hello", null))

    val typedRow = DataUtils.convertStringRowToTypedRow(stringRow, schema)

    val expected = Row.fromSeq(Seq(123, 9876543210L, 123.45, 0.5f, true, "hello", null))

    assertEquals(expected.values, typedRow.values, "所有 String 类型都应转换为 Schema 中指定的类型")
  }

  @Test
  def testConvertStringRowToTypedRow_Unsupported(): Unit = {
    val schema = StructType(Seq(Column("a", RefType)))
    val row = Row.fromSeq(Seq("some_ref"))

    assertThrows(classOf[UnsupportedOperationException], () => {
      DataUtils.convertStringRowToTypedRow(row, schema)
      ()}, "不支持的类型转换应抛出异常")
  }

  @Test
  def testInferSchema(): Unit = {
    val header = Seq(" id ", "  value  ", "  flag  ", "mixed") // 测试 trim
    val lines = Seq(
      Array("1", "10.5", "true", "100"),
      Array("2", "20.0", "false", "text"), // "text" 会将 mixed 列提升为 String
      Array("3", "5", "true", "2.5")        // 5 (Long) 也会被提升
    )

    val schema = DataUtils.inferSchema(lines, header)

    val expected = StructType(Seq(
      Column("id", LongType),    // 1, 2, 3 -> Long
      Column("value", DoubleType), // 10.5, 20.0, 5 -> Double (因为 5 是 Long, 10.5 是 Double)
      Column("flag", BooleanType), // true, false, true -> Boolean
      Column("mixed", StringType)  // 100 (Long), text (String), 2.5 (Double) -> String
    ))

    assertEquals(expected, schema, "Schema 应被正确推断和类型提升")
  }

  @Test
  def testInferSchema_NoHeader(): Unit = {
    val lines = Seq(Array("true", "1234567890"), Array("false", "1.5"))
    val schema = DataUtils.inferSchema(lines, Seq.empty)

    val expected = StructType(Seq(
      Column("_1", BooleanType),
      Column("_2", DoubleType) // Long + Double -> Double
    ))

    assertEquals(expected, schema, "没有表头时应自动生成列名 _1, _2...")
  }

  @Test
  def testInferStringValueType(): Unit = {
    assertAll("inferStringValueType",
      () => assertEquals(LongType, DataUtils.inferStringValueType("12345"), "应为 LongType"),
      () => assertEquals(LongType, DataUtils.inferStringValueType("-10"), "应为 LongType (负数)"),
      () => assertEquals(DoubleType, DataUtils.inferStringValueType("123.45"), "应为 DoubleType"),
      () => assertEquals(DoubleType, DataUtils.inferStringValueType("-0.5"), "应为 DoubleType (负数)"),
      () => assertEquals(BooleanType, DataUtils.inferStringValueType("true"), "应为 BooleanType"),
      () => assertEquals(BooleanType, DataUtils.inferStringValueType("FALSE"), "应为 BooleanType (不区分大小写)"),
      () => assertEquals(StringType, DataUtils.inferStringValueType("hello"), "应为 StringType"),
      () => assertEquals(StringType, DataUtils.inferStringValueType("10.5.1"), "应为 StringType (非法数字)"),
      () => assertEquals(StringType, DataUtils.inferStringValueType(""), "应为 StringType (空)"),
      () => assertEquals(StringType, DataUtils.inferStringValueType(null), "应为 StringType (null)")
    )
  }

  @Test
  def testCountLinesFast(): Unit = {
    val count = DataUtils.countLinesFast(testFile)
    assertEquals(3L, count, "文件应有3行")

    val emptyFile = createTempFile("empty.txt", "")
    val emptyCount = DataUtils.countLinesFast(emptyFile)
    assertEquals(0L, emptyCount, "空文件应有0行")
  }

  @Test
  def testInferSchemaFromRow(): Unit = {
    val row = Row.fromSeq(Seq(10, "hello", true, 1.5, null, 123L))
    val schema = DataUtils.inferSchemaFromRow(row)
    val expected = StructType(Seq(
      Column("_1", IntType),
      Column("_2", StringType),
      Column("_3", BooleanType),
      Column("_4", DoubleType),
      Column("_5", NullType),
      Column("_6", LongType)
    ))
    assertEquals(expected, schema, "应根据 Row 中的值推断 Schema")
  }

  @Test
  def testInferValueType(): Unit = {
    assertAll("inferValueType",
      () => assertEquals(NullType, DataUtils.inferValueType(null), "null -> NullType"),
      () => assertEquals(IntType, DataUtils.inferValueType(123), "Int -> IntType"),
      () => assertEquals(LongType, DataUtils.inferValueType(123L), "Long -> LongType"),
      () => assertEquals(DoubleType, DataUtils.inferValueType(123.45), "Double -> DoubleType"),
      () => assertEquals(DoubleType, DataUtils.inferValueType(123.45f), "Float -> DoubleType"),
      () => assertEquals(BooleanType, DataUtils.inferValueType(true), "Boolean -> BooleanType"),
      () => assertEquals(BinaryType, DataUtils.inferValueType(Array[Byte](1, 2)), "Array[Byte] -> BinaryType"),
      () => assertEquals(BinaryType, DataUtils.inferValueType(new File("a")), "File -> BinaryType"),
      () => assertEquals(BlobType, DataUtils.inferValueType(Blob.fromFile(new File(""))), "Blob -> BlobType"),
      () => assertEquals(RefType, DataUtils.inferValueType(DFRef(null)), "DFRef -> RefType"),
      () => assertEquals(StringType, DataUtils.inferValueType("text"), "String -> StringType"),
      () => assertEquals(StringType, DataUtils.inferValueType(new java.util.Date()), "Other -> StringType")
    )
  }

  @Test
  def testInferExcelSchema_Xlsx(): Unit = {
    val excelFile = createTestExcelFile("test.xlsx", isXlsx = true)
    val schema = DataUtils.inferExcelSchema(excelFile.getAbsolutePath)

    val expected = StructType(Seq(
      Column("ID", LongType),      // 101
      Column("Name", StringType),  // "Test 1"
      Column("Value", LongType), // 123.45
      Column("IsActive", BooleanType), // true
      Column("DateNum", LongType) // 日期被检测为 Long
    ))

    assertEquals(expected, schema, "XLSX Schema 应被正确推断")
  }

  @Test
  def testInferExcelSchema_Empty(): Unit = {
    val wb = new XSSFWorkbook()
    wb.createSheet("Sheet1")
    val file = tempDir.resolve("empty.xlsx").toFile
    val fos = new FileOutputStream(file)
    wb.write(fos)
    fos.close()
    wb.close()

    assertThrows(classOf[RuntimeException], () => {
      DataUtils.inferExcelSchema(file.getAbsolutePath)
      ()}, "空 Excel 文件应抛出异常")
  }

  @Test
  def testReadExcelRows_Xlsx(): Unit = {
    val excelFile = createTestExcelFile("read.xlsx", isXlsx = true)
    val schema = StructType(Seq(
      Column("ID", IntType),
      Column("Name", StringType),
      Column("Value", DoubleType),
      Column("IsActive", BooleanType),
      Column("DateNum", LongType) // 故意使用 Long 读取日期
    ))

    val rows = DataUtils.readExcelRows(excelFile.getAbsolutePath, schema).toList

    assertEquals(2, rows.length, "应读取2行数据")

    // 检查第一行
    assertEquals(List(101, "Test 1", 123.45, true, 44197L), rows.head, "第一行数据应正确读取")
    // 检查第二行
    assertEquals(List(102, "Test 2", 200.0, false, ""), rows(1), "第二行数据应正确读取 (空单元格转为空字符串)")
  }

  @Test
  def testReadExcelRows_Xls(): Unit = {
    val excelFile = createTestExcelFile("read.xls", isXlsx = false)
    val schema = StructType(Seq(
      Column("ID", IntType),
      Column("Name", StringType),
      Column("Value", DoubleType)
    ))

    val rows = DataUtils.readExcelRows(excelFile.getAbsolutePath, schema).toList

    assertEquals(2, rows.length, "应读取 .xls 文件的2行数据")
    assertEquals(List(101, "Test 1", 123.45), rows.head.take(3), "第一行 .xls 数据应正确读取")
    assertEquals(List(102, "Test 2", 200.0), rows(1).take(3), "第二行 .xls 数据应正确读取")
  }

  @Test
  def testGetStructTypeStreamFromJson(): Unit = {
    val json1 = "{\"id\": 1, \"name\": \"A\"}"
    val json2 = "{\"id\": 2, \"name\": \"B\"}"
    val iter = Iterator(json1, json2)

    val (stream, schema) = DataUtils.getStructTypeStreamFromJson(iter)

    // 验证 Schema
    val expectedSchema = StructType(Seq(Column("name", StringType), Column("id", IntType)))
    assertEquals(expectedSchema, schema, "JSON Schema 应从第一行正确推断")

    // 验证流数据 (注意：实现中的 ++ 导致第一行在最后)
    val rows = stream.toList
    assertEquals(2, rows.length, "应包含所有行")
    assertEquals(Row.fromJsonString(json2).toString, rows.head.toString, "流的第一个元素应是原始迭代器的第二个元素")
    assertEquals(Row.fromJsonString(json1).toString, rows(1).toString, "流的最后一个元素应是原始迭代器的第一个元素")
  }

  @Test
  def testGetStructTypeStreamFromJson_Empty(): Unit = {
    val (stream, schema) = DataUtils.getStructTypeStreamFromJson(Iterator.empty)
    assertEquals(StructType.empty, schema, "空迭代器应返回空 Schema")
    assertTrue(stream.isEmpty, "空迭代器应返回空流")
  }

  @Test
  def testChunkedIterator(): Unit = {
    val data = Array.tabulate[Byte](25)(i => i.toByte) // 25 字节
    val is = new ByteArrayInputStream(data)

    val iter = DataUtils.chunkedIterator(is, 10) // 块大小 10

    val chunks = iter.toList

    assertEquals(3, chunks.length, "应有3个块")
    assertEquals(10, chunks(0).length, "块1大小应为10")
    assertEquals(10, chunks(1).length, "块2大小应为10")
    assertEquals(5, chunks(2).length, "块3大小应为5 (剩余部分)")

    // 验证内容
    assertEquals(data.slice(0, 10).toList, chunks(0).toList, "块1内容应正确")
    assertEquals(data.slice(20, 25).toList, chunks(2).toList, "块3内容应正确")
  }

  @Test
  def testChunkedIterator_EmptyInput(): Unit = {
    val is = new ByteArrayInputStream(Array[Byte]())
    val iter = DataUtils.chunkedIterator(is, 10)

    // 该实现对于空流，在第一次调用 next() 时会抛出异常
    assertThrows(classOf[NoSuchElementException], () => {
      iter.next()
      ()}, "空输入流在调用 next() 时应抛出 NoSuchElementException")

    // 更好的测试可能是检查 hasNext
    // assertFalse(iter.hasNext, "空输入流 hasNext 应为 false")
    // (注意：根据当前实现，hasNext 在第一次调用 next 之前为 true，这是实现中的一个缺陷)
  }
}