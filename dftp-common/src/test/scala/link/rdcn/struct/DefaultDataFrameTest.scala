/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:53
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.util.{DataUtils, ResourceUtils}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.concurrent.atomic.AtomicInteger

class DefaultDataFrameTest {

  private val originalSchema = StructType(List(Column("id", ValueType.IntType), Column("name", ValueType.StringType)))
  private val mockRows = List(Row(1, "Alice"), Row(2, "Bob"), Row(3, "Charlie"))
  private var closeCounter: AtomicInteger = _
  private var mockStream: ClosableIterator[Row] = _
  private var df: DefaultDataFrame = _

  @BeforeEach
  def setUp(): Unit = {
    closeCounter = new AtomicInteger(0)
    // 创建带有可跟踪 close 行为的 stream
    mockStream = ClosableIterator(mockRows.iterator)(closeCounter.incrementAndGet())
    df = DefaultDataFrame(originalSchema, mockStream)
  }

  @Test
  def testMapOperation(): Unit = {
    // 覆盖 map 方法
    val mappedDf = df.map(row => Row(row.get(0).asInstanceOf[Int] * 2))

    // 验证新 Stream 的关闭逻辑：原始流的 close() 应该被传递
    val transformedRows = mappedDf.collect()

    assertEquals(1, closeCounter.get(), "New stream's close must invoke original stream's close()")
    assertEquals(2, transformedRows.head.values.head, "Mapped stream should contain transformed data (1*2=2)")
  }

  @Test
  def testFilterOperation(): Unit = {
    // 覆盖 filter 方法
    val filteredDf = df.filter(row => row.get(0).asInstanceOf[Int] > 1)

    // 验证数据转换：通过执行新的流来验证 filter 操作
    val newStream = filteredDf.asInstanceOf[DefaultDataFrame].stream
    val filteredRows = newStream.toList

    assertEquals(2, filteredRows.size, "Filtered stream should contain 2 rows (2 and 3)")
    assertEquals(2, filteredRows.head.values.head, "Filtered stream should start with row 2")
  }

  @Test
  def testSelectOperation_Success(): Unit = {
    val columnsToSelect = Seq("name", "id")
    // 覆盖 select 方法的成功路径
    val selectedDf = df.select(columnsToSelect: _*)

    assertTrue(selectedDf.isInstanceOf[DefaultDataFrame], "Select should return a DefaultDataFrame")

    // 验证新 Schema
    val newSchema = selectedDf.schema
    assertEquals(2, newSchema.columns.size, "New schema should contain 2 columns")
    assertEquals("name", newSchema.columns.head.name, "New schema should respect order: name, id")

    // 验证数据转换
    val collectedRows = selectedDf.collect() // 触发数据流
    assertEquals("Alice", collectedRows.head.values.head, "Data should be transformed and reordered: name")
    assertEquals(1, collectedRows.head.values(1), "Data should be transformed and reordered: id")

    // 验证关闭继承
    assertEquals(1, closeCounter.get(), "Collect on selectedDf should close the original stream")
  }

  @Test
  def testSelectOperation_InvalidColumn(): Unit = {
    // 覆盖 select 方法的异常路径
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => df.select("id", "non_existent_col")
    )

    assertEquals("StructType.select: 列名 'non_existent_col' 不存在", exception.getMessage, "Exception should indicate the missing column name")
  }

  @Test
  def testLimitOperation(): Unit = {
    val limitN = 2
    // 覆盖 limit 方法
    val limitedDf = df.limit(limitN)

    assertTrue(limitedDf.isInstanceOf[DefaultDataFrame], "Limit should return a DefaultDataFrame")
    assertEquals(originalSchema, limitedDf.schema, "Schema should be preserved by limit")

    // 验证数据被限制
    val collectedRows = limitedDf.collect()
    assertEquals(limitN, collectedRows.size, "Limited DataFrame should contain only N rows")

    // 验证关闭继承
    assertEquals(1, closeCounter.get(), "Collect on limitedDf should close the original stream")
  }

  @Test
  def testForeach_UsesResourceUtilsAndCloses(): Unit = {
    var count = 0
    // 覆盖 foreach 方法
    df.foreach(_ => count += 1)

    assertEquals(mockRows.size, count, "Foreach must iterate over all rows")
    assertEquals(1, closeCounter.get(), "Foreach must close the stream via ResourceUtils")
  }

  @Test
  def testCollect_UsesResourceUtilsAndCloses(): Unit = {
    // 覆盖 collect 方法
    val collected = df.collect()

    assertEquals(mockRows.size, collected.size, "Collect must return all rows")
    assertEquals(1, closeCounter.get(), "Collect must close the stream via ResourceUtils")
  }

  @Test
  def testMapIterator(): Unit = {
    // 覆盖 mapIterator[T] 方法
    val result: Int = df.mapIterator(iter=>iter.size)

    assertEquals(mockRows.size, result, "MapIterator should receive and count the stream size")
  }
}