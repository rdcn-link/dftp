/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class DataFrameObjectTest {

  @Test
  def testCreateFromDataStreamSource(): Unit = {
    val expectedSchema = StructType(List(Column("A", ValueType.IntType)))
    val mockIterator = ClosableIterator(List(Row.fromSeq(Seq(1))).iterator,()=>(),false)
    val mockSource = new DataStreamSource {
      override def rowCount: Long = 0L

      override def schema: StructType = expectedSchema

      override def iterator: ClosableIterator[Row] = mockIterator
    }

    // 覆盖 create(dataStreamSource)
    val df = DataFrame.create(mockSource)

    assertTrue(df.isInstanceOf[DefaultDataFrame], "Created object must be DefaultDataFrame")
    assertEquals(expectedSchema, df.schema, "DataFrame should use the schema from DataStreamSource")

    val dfIter = df.asInstanceOf[DefaultDataFrame].stream
    assertTrue(dfIter eq mockIterator, "DataFrame should use the iterator from DataStreamSource (reference equality)")
  }

  @Test
  def testFromSeq(): Unit = {
    val inputSeq = Seq(1, 2, "three")

    // 覆盖 fromSeq(seq)
    val df = DataFrame.fromSeq(inputSeq)

    // 验证数据内容
    val collectedRows = df.asInstanceOf[DefaultDataFrame].collect()
    assertEquals(inputSeq.size, collectedRows.size, "DataFrame should contain all elements from input sequence")
    assertEquals(inputSeq.head, collectedRows.head.values.head, "First row value must match first element of input seq")
  }

  @Test
  def testFromMap(): Unit = {
    val inputMaps = Seq(
      Map("name" -> "Alice", "age" -> 30),
      Map("name" -> "Bob", "age" -> 25)
    )

    // 覆盖 fromMap(maps)
    val df = DataFrame.fromMap(inputMaps)

    // 验证 Schema 是否正确推断 (基于 Mock)
    val expectedSchema = StructType(List(Column("name", ValueType.StringType), Column("age", ValueType.IntType)))
    assertEquals(expectedSchema, df.schema, "DataFrame should use the schema inferred from the first map's keys")

    // 验证数据内容
    val collectedRows = df.asInstanceOf[DefaultDataFrame].collect()
    assertEquals(inputMaps.size, collectedRows.size, "DataFrame should contain all input maps")

    // 验证第一行内容
    assertEquals(2, collectedRows.head.values.size, "Each row should contain 2 elements (map values)")
  }

  @Test
  def testEmptyDataFrame(): Unit = {
    // 覆盖 empty()
    val df = DataFrame.empty()

    assertTrue(df.isInstanceOf[DefaultDataFrame], "Empty object must be DefaultDataFrame")
    assertEquals(StructType.empty, df.schema, "Empty DataFrame should have empty schema")

    val dfIter = df.asInstanceOf[DefaultDataFrame].stream.underlying
    assertTrue(!dfIter.hasNext, "Iterator must be empty")
  }
}
