/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 11:08
 * @Modified By:
 */
package link.rdcn.client

import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.apache.arrow.flight.{PutResult, Result, SyncPutListener}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.{FloatingPointPrecision, Types}
import org.apache.arrow.vector.types.pojo._
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api._

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.collection.JavaConverters._

class ClientUtilsJunitTest {
  private val allocator = new RootAllocator(Long.MaxValue)

  @Test
  def testArrowSchemaToStructType_PrimitiveTypes(): Unit = {
    // 覆盖 Int, Long, Float, Double, Boolean 的 case
    val arrowFields = List(
      new Field("c1", new FieldType(true, Types.MinorType.INT.getType, null), Collections.emptyList()),
      new Field("c2", new FieldType(true, Types.MinorType.BIGINT.getType, null), Collections.emptyList()),
      new Field("c3", new FieldType(true, Types.MinorType.FLOAT4.getType, null), Collections.emptyList()),
      new Field("c4", new FieldType(true, Types.MinorType.FLOAT8.getType, null), Collections.emptyList()),
      new Field("c5", new FieldType(true, Types.MinorType.BIT.getType, null), Collections.emptyList())
    ).asJava

    val arrowSchema = new Schema(arrowFields)
    val structType = ClientUtils.arrowSchemaToStructType(arrowSchema)

    val expectedColumns = List(
      Column("c1", IntType),
      Column("c2", LongType),
      Column("c3", FloatType),
      Column("c4", DoubleType),
      Column("c5", BooleanType)
    )

    assertEquals(StructType(expectedColumns), structType)
  }

  @Test
  def testArrowSchemaToStructTypeStringAndBinaryTypes(): Unit = {
    // 覆盖 VARCHAR, VARBINARY 的普通类型和带元数据的特殊类型 (RefType, BlobType)
    val refMetadata = Map("logicalType" -> "Url").asJava
    val blobMetadata = Map("logicalType" -> "blob").asJava

    val arrowFields = List(
      // 普通 StringType (VARCHAR, 无元数据)
      new Field("c1", new FieldType(true, Types.MinorType.VARCHAR.getType, null), Collections.emptyList()),
      // RefType (VARCHAR, 有元数据)
      new Field("c2", new FieldType(true, Types.MinorType.VARCHAR.getType, null, refMetadata), Collections.emptyList()),
      // 普通 BinaryType (VARBINARY, 无元数据)
      new Field("c3", new FieldType(true, Types.MinorType.VARBINARY.getType, null), Collections.emptyList()),
      // BlobType (VARBINARY, 有元数据)
      new Field("c4", new FieldType(true, Types.MinorType.VARBINARY.getType, null, blobMetadata), Collections.emptyList())
    ).asJava

    val arrowSchema = new Schema(arrowFields)
    val structType = ClientUtils.arrowSchemaToStructType(arrowSchema)

    val expectedColumns = List(
      Column("c1", StringType),
      Column("c2", RefType),
      Column("c3", BinaryType),
      Column("c4", BlobType)
    )

    assertEquals(StructType(expectedColumns), structType)
  }

  @Test()
  def testArrowSchemaToStructTypeUnsupportedType(): Unit = {
    // 覆盖 default case
    val unsupportedType = Types.MinorType.DATEDAY.getType // 使用一个未处理的 Arrow Type
    val arrowFields = List(
      new Field("c1", new FieldType(true, unsupportedType, null), Collections.emptyList())
    ).asJava
    val arrowSchema = new Schema(arrowFields)
    val exception = assertThrows(
      classOf[UnsupportedOperationException], () => ClientUtils.arrowSchemaToStructType(arrowSchema))
    assertEquals(exception.getMessage, s"Unsupported Arrow type: ${unsupportedType}")

  }

  @Test
  def testGetVectorSchemaRootFromBytes(): Unit = {
    val field = Field.nullable("data", Types.MinorType.INT.getType)
    val schema = new Schema(Collections.singletonList(field))
    val root = VectorSchemaRoot.create(schema, allocator)

    val intVector = root.getVector("data").asInstanceOf[IntVector]
    intVector.allocateNew()
    intVector.setSafe(0, 123)
    root.setRowCount(1)

    val outputStream = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, outputStream)
    writer.writeBatch()
    writer.end()

    val bytes = outputStream.toByteArray
    outputStream.close()

    val newRoot = ClientUtils.getVectorSchemaRootFromBytes(bytes, allocator)

    assertTrue(newRoot.getRowCount > 0, "Should load the next batch")
    assertEquals(schema, newRoot.getSchema, "Schema should match")

    newRoot.close()
    root.close()
    writer.close()
  }

  @Test
  def testConvertStructTypeToArrowSchemaAllTypes(): Unit = {
    // 覆盖所有的 ValueType case: Int, Long, Float, Double, String, Boolean, Binary, Ref, Blob
    val structType = StructType(List(
      Column("c1", IntType, nullable = false), // IntType, 32 bit, non-nullable
      Column("c2", LongType), // LongType, 64 bit
      Column("c3", FloatType), // FloatType, single precision
      Column("c4", DoubleType), // DoubleType, double precision
      Column("c5", StringType), // StringType, Utf8
      Column("c6", BooleanType), // BooleanType, Bool
      Column("c7", BinaryType), // BinaryType, Binary
      Column("c8", RefType), // RefType, Utf8 with metadata
      Column("c9", BlobType) // BlobType, Binary with metadata
    ))

    val arrowSchema = ClientUtils.convertStructTypeToArrowSchema(structType)
    val fields = arrowSchema.getFields.asScala.toList

    assertEquals(Types.MinorType.INT.getType, fields(0).getType)
    assertTrue(!fields(0).isNullable)

    assertEquals(Types.MinorType.BIGINT.getType, fields(1).getType)

    assertTrue(fields(2).getType.isInstanceOf[ArrowType.FloatingPoint])
    assertEquals(FloatingPointPrecision.SINGLE, fields(2).getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision)

    assertTrue(fields(3).getType.isInstanceOf[ArrowType.FloatingPoint])
    assertEquals(FloatingPointPrecision.DOUBLE, fields(3).getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision)

    assertEquals(ArrowType.Utf8.INSTANCE, fields(4).getType)
    assertTrue(fields(4).getMetadata.isEmpty)

    assertEquals(ArrowType.Bool.INSTANCE, fields(5).getType)

    assertEquals(new ArrowType.Binary(), fields(6).getType)
    assertTrue(fields(6).getMetadata.isEmpty)

    assertEquals(ArrowType.Utf8.INSTANCE, fields(7).getType)
    assertEquals("Url", fields(7).getMetadata.get("logicalType"))

    assertEquals(new ArrowType.Binary(), fields(8).getType)
    assertEquals("blob", fields(8).getMetadata.get("logicalType"))
  }

  @Test
  def testParsePutListenerNoMetadata(): Unit = {
    val putListener = new SyncPutListener() // 实例化一个真实的类，不向其写入任何内容
    val queueField = classOf[SyncPutListener].getDeclaredField("queue")
    queueField.setAccessible(true)
    val queue = queueField.get(putListener).asInstanceOf[java.util.concurrent.BlockingQueue[PutResult]]
    queue.clear() // 确保队列为空
    var result: Option[Array[Byte]] = None
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        // 阻塞操作在单独线程中执行
        result = ClientUtils.parsePutListener(putListener)
      }
    })
    thread.start()
    Thread.sleep(100)
    thread.interrupt()
    thread.join(500)
    assertTrue(!thread.isAlive, "Test thread should have stopped after interrupt")
    assertTrue(result.isEmpty, "Result should be None or handle the interruption gracefully")
  }

  @Test
  def testParseFlightActionResultsAllVectorTypes(): Unit = {
    // 创建一个包含所有支持 Vector 类型的 Schema 和数据
    val metadata = new java.util.HashMap[String, String]()
    metadata.put("logicalType", "Url")
    val schema = new Schema(List(
      Field.nullable("int_col", Types.MinorType.INT.getType),
      Field.nullable("long_col", Types.MinorType.BIGINT.getType),
      Field.nullable("float_col", Types.MinorType.FLOAT4.getType),
      Field.nullable("double_col", Types.MinorType.FLOAT8.getType),
      Field.nullable("bool_col", Types.MinorType.BIT.getType),
      Field.nullable("bin_col", Types.MinorType.VARBINARY.getType),
      Field.nullable("str_col", Types.MinorType.VARCHAR.getType),
      new Field("ref_col", new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata), Collections.emptyList())

    ).asJava)

    val root = VectorSchemaRoot.create(schema, allocator)
    root.allocateNew()

    // 设置数据，确保覆盖 null 和非 null 值
    root.getVector("int_col").asInstanceOf[IntVector].setSafe(0, 1)
    root.getVector("int_col").asInstanceOf[IntVector].setSafe(1, 2)
    root.getVector("int_col").asInstanceOf[IntVector].setNull(2) // 覆盖 null 分支

    root.getVector("long_col").asInstanceOf[BigIntVector].setSafe(0, 100L)
    root.getVector("long_col").asInstanceOf[BigIntVector].setSafe(1, 200L)

    root.getVector("float_col").asInstanceOf[Float4Vector].setSafe(0, 1.1f)
    root.getVector("float_col").asInstanceOf[Float4Vector].setSafe(1, 2.2f)

    root.getVector("double_col").asInstanceOf[Float8Vector].setSafe(0, 10.1)
    root.getVector("double_col").asInstanceOf[Float8Vector].setSafe(1, 20.2)

    root.getVector("bool_col").asInstanceOf[BitVector].setSafe(0, 1) // true
    root.getVector("bool_col").asInstanceOf[BitVector].setSafe(1, 0) // false

    root.getVector("bin_col").asInstanceOf[VarBinaryVector].setSafe(0, "binary1".getBytes(StandardCharsets.UTF_8))
    root.getVector("bin_col").asInstanceOf[VarBinaryVector].setSafe(1, "binary2".getBytes(StandardCharsets.UTF_8))

    root.getVector("str_col").asInstanceOf[VarCharVector].setSafe(0, "hello".getBytes(StandardCharsets.UTF_8))
    root.getVector("str_col").asInstanceOf[VarCharVector].setSafe(1, "world".getBytes(StandardCharsets.UTF_8))

    root.getVector("ref_col").asInstanceOf[VarCharVector].setSafe(0, "ref1".getBytes(StandardCharsets.UTF_8))
    root.getVector("ref_col").asInstanceOf[VarCharVector].setSafe(1, "ref2".getBytes(StandardCharsets.UTF_8))

    root.setRowCount(3)

    // 写入 Arrow 格式字节数组
    val outputStream = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, outputStream)
    writer.writeBatch()
    writer.end()
    val resultBytes = outputStream.toByteArray
    root.close()
    writer.close()

    // 模拟 Result 迭代器
    val mockResult = new Result(resultBytes)
    val resultIterator: java.util.Iterator[Result] = Collections.singletonList(mockResult).iterator()

    // 执行解析
    val dataFrame = ClientUtils.parseFlightActionResults(resultIterator, allocator)

    // 检查 Schema
    assertEquals(IntType, dataFrame.schema.columns.find(_.name == "int_col").get.colType)
    assertEquals(RefType, dataFrame.schema.columns.find(_.name == "ref_col").get.colType) // 验证 StructType 转换

    // 检查数据行
    val rows = dataFrame.collect()
    assertEquals(3, rows.size, "Should have 3 rows")

    // 检查第一行
    val row0 = rows.head
    assertEquals(1, row0._1)
    assertEquals(100L, row0._2)
    assertEquals(1.1f, row0._3)
    assertEquals(10.1, row0._4)
    assertEquals(true, row0._5)
    assertArrayEquals("binary1".getBytes(StandardCharsets.UTF_8), row0._6.asInstanceOf[Array[Byte]])
    assertEquals("hello", row0._7)
    assertEquals(DFRef("ref1"), row0._8) // 验证 DFRef 转换

    // 检查第二行
    val row1 = rows(1)
    assertEquals(2, row1._1)
    assertEquals(200L, row1._2)
    assertEquals(2.2f, row1._3)
    assertEquals(20.2, row1._4)
    assertEquals(false, row1._5)
    assertArrayEquals("binary2".getBytes(StandardCharsets.UTF_8), row1._6.asInstanceOf[Array[Byte]])
    assertEquals("world", row1._7)
    assertEquals(DFRef("ref2"), row1._8)

    // 检查第三行 (包含 null 值)
    val row2 = rows(2)
    assertTrue(row2._1 == null, "Null value should be null")
  }

  @Test
  def testParseFlightActionResultsUnsupportedVectorType(): Unit = {
    // 覆盖 default case
    val unsupportedField = Field.nullable("unsupported", Types.MinorType.DATEDAY.getType) // 使用一个未处理的 Vector Type
    val schema = new Schema(Collections.singletonList(unsupportedField))

    val root = VectorSchemaRoot.create(schema, allocator)
    root.allocateNew()
    root.setRowCount(1)

    val outputStream = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, outputStream)
    writer.writeBatch()
    writer.end()
    val resultBytes = outputStream.toByteArray
    root.close()
    writer.close()

    val mockResult = new Result(resultBytes)
    val resultIterator: java.util.Iterator[Result] = Collections.singletonList(mockResult).iterator()
    val exception = assertThrows(
      classOf[UnsupportedOperationException], () => ClientUtils.parseFlightActionResults(resultIterator, allocator))
    assertEquals(s"Unsupported Arrow type: ${Types.MinorType.DATEDAY.getType}", exception.getMessage)
  }
}
