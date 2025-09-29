package link.rdcn.server

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:49
 * @Modified By:
 */
import link.rdcn.server.ArrowFlightStreamWriterTest.{allocator, root, testRow}
import link.rdcn.struct.{ArrowFlightStreamWriter, Blob, BlobRegistry, DFRef, Row}
import link.rdcn.util.CodecUtils
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo._
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, Test}

import java.io.InputStream
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}

object ArrowFlightStreamWriterTest {
  private val ALL_FIELD_NAMES = List("int", "long", "double", "float", "decimal", "string", "boolean", "binary", "ref", "blob")
  private val fields: java.lang.Iterable[Field] = List(
    Field.nullable(ALL_FIELD_NAMES(0), Types.MinorType.INT.getType),
    Field.nullable(ALL_FIELD_NAMES(1), Types.MinorType.BIGINT.getType),
    Field.nullable(ALL_FIELD_NAMES(2), Types.MinorType.FLOAT8.getType),
    Field.nullable(ALL_FIELD_NAMES(3), Types.MinorType.FLOAT4.getType),
    Field.nullable(ALL_FIELD_NAMES(4), Types.MinorType.VARCHAR.getType), // BigDecimal 编码为 String/VarChar
    Field.nullable(ALL_FIELD_NAMES(5), Types.MinorType.VARCHAR.getType),
    Field.nullable(ALL_FIELD_NAMES(6), Types.MinorType.BIT.getType),
    Field.nullable(ALL_FIELD_NAMES(7), Types.MinorType.VARBINARY.getType),
    Field.nullable(ALL_FIELD_NAMES(8), Types.MinorType.VARCHAR.getType), // DFRef 编码为 String/VarChar
    Field.nullable(ALL_FIELD_NAMES(9), Types.MinorType.VARBINARY.getType) // Blob 编码为 Byte[]
  ).asJava

  private val schema = new Schema(fields)
  private val allocator: BufferAllocator = new RootAllocator()
  private val root: VectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
  val testBlob = new Blob{
    override def offerStream[T](consume: InputStream => T): T = {
      val stream = new InputStream {
        override def read(): Int = ???
      }
      try consume(stream)
      finally stream.close()
    }
  }

  // 创建一行包含所有支持类型的 Row，并覆盖所有 case 分支
  val testRow = Row(
    10: Int, // Int
    20L: Long, // Long
    30.5: Double, // Double
    40.5f: Float, // Float
    new BigDecimal("50.5"), // BigDecimal (编码为 VarChar)
    "test_str", // String (编码为 VarChar)
    true: Boolean, // Boolean (编码为 BitVector)
    "raw_bytes".getBytes(StandardCharsets.UTF_8), // Array[Byte] (编码为 VarBinary)
    DFRef("http://ref"), // DFRef (编码为 VarChar)
    testBlob // Blob (编码为 VarBinary)
  )

  @AfterAll
  def tearDown(): Unit = {
    if (root != null) root.close()
    if (allocator != null) allocator.close()
  }
}

class ArrowFlightStreamWriterTest {

  @Test
  def testProcessGroupsRowsCorrectly(): Unit = {
    val allRows = List(testRow,testRow,testRow)

    val writer = ArrowFlightStreamWriter(allRows.iterator)

    // 测试 batchSize = 2
    val batches = writer.process(root, 2).toList

    assertEquals(2, batches.size, "Process should produce 2 batches (2 + 1)")
    assertEquals(2, batches.head.getLength, "First batch row count should be 2")
    assertEquals(1, batches(1).getLength, "Second batch row count should be 1")

    // 清理资源
    batches.foreach(_.close())
  }

  @Test
  def testCreateDummyBatch_AllTypesCovered(): Unit = {
    val writer = ArrowFlightStreamWriter(List(testRow).iterator)
    val batch = writer.process(root, 1).next() // 触发 createDummyBatch

    // 验证行数
    assertEquals(1, root.getRowCount, "Row count should be 1")

    // 验证 IntVector
    val intVector = root.getVector("int").asInstanceOf[IntVector]
    assertEquals(10, intVector.get(0), "Int value must match")

    // 验证 LongVector
    val longVector = root.getVector("long").asInstanceOf[BigIntVector]
    assertEquals(20L, longVector.get(0), "Long value must match")

    // 验证 BooleanVector
    val bitVector = root.getVector("boolean").asInstanceOf[BitVector]
    assertEquals(1, bitVector.get(0), "Boolean value must be 1 (true)")

    // 验证 Blob 注册和编码 (VarBinaryVector)
    val blobVector = root.getVector("blob").asInstanceOf[VarBinaryVector]
    assertTrue(BlobRegistry.getBlob(CodecUtils.decodeString(blobVector.get(0))).getOrElse(null) != null, "Blob should be registered and encoded as Byte[]")

    // 验证 DFRef
    val refVector = root.getVector("ref").asInstanceOf[VarCharVector]
    assertArrayEquals("http://ref".getBytes(StandardCharsets.UTF_8), refVector.get(0), "DFRef should be encoded as URL bytes")

    // 清理资源
    batch.close()
    BlobRegistry.cleanUp()
  }

  @Test
  def testCreateDummyBatchNullValue(): Unit = {
    // 覆盖 case null => vec.setNull(i)
    val testRow = Row(null, null, null, null, null, null, null, null, null, null)
    val writer = ArrowFlightStreamWriter(List(testRow).iterator)
    writer.process(root, 1).next().close() // 触发 createDummyBatch

    // 验证所有字段是否设置为 null
    root.allocateNew()
    val allVectors = root.getFieldVectors.asScala

    allVectors.foreach(vec => {
      assertTrue(vec.isNull(0),s"Vector ${vec.getName} should be null at index 0")
    })
  }

  @Test()
  def testCreateDummyBatchUnsupportedType(): Unit = {
    class UnsupportedType
    val testRow = Row(Seq(10, 20L, new UnsupportedType()))

    // 创建一个只包含 Int, Long 和 VarChar 的 root
    val unsupportedRoot = VectorSchemaRoot.create(
      new Schema(List(
        Field.nullable("i", Types.MinorType.INT.getType),
        Field.nullable("l", Types.MinorType.BIGINT.getType),
        Field.nullable("u", Types.MinorType.VARCHAR.getType) // 预期在这里抛出异常
      ).asJava), allocator)

    try {
      val writer = ArrowFlightStreamWriter(List(testRow).iterator)
      // 尝试处理，预期在 VarCharVector 尝试处理 UnsupportedType 时抛出异常

      val exception = assertThrows(
        classOf[UnsupportedOperationException], () => writer.process(unsupportedRoot, 1).next().close())
      assertEquals(exception.getMessage, "Type not supported")

    } finally {
      unsupportedRoot.close()
    }
  }
}