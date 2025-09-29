package link.rdcn.client

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 09:15
 * @Modified By:
 */

import link.rdcn.ClientTestProvider._
import link.rdcn.ClientTestBase._
import link.rdcn.ClientTestProvider
import link.rdcn.operation._
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.struct._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Paths
import scala.io.{BufferedSource, Source}

class RemoteDataFrameProxyTest extends ClientTestProvider {

  private val initialSchema = StructType(List(Column("id", LongType), Column("value", DoubleType)))

  private val remoteDataFrame = dc.get("dftp://localhost:3101/csv/data_1.csv").asInstanceOf[RemoteDataFrameProxy]
  private var source: BufferedSource = _
  private var size: Int = _

  @BeforeEach
  def setUp(): Unit = {
    source = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString)
    size = source.getLines().size - 1
    source.close()
    source = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString)
  }

  @AfterEach
  def close(): Unit = {
    source.close()
  }

  @Test
  def testInitializationAndSchema(): Unit = {
    // 覆盖 val schema: StructType = getRows(operation.toJsonString)._1
    assertEquals(initialSchema, remoteDataFrame.schema, "Schema should be initialized correctly upon construction")
    assertTrue(remoteDataFrame.operation.isInstanceOf[SourceOp], "Initial operation should be the BaseOp")
  }

  @Test
  def testFilterOperationChain(): Unit = {
    // 覆盖 filter 方法
    val filteredDataFrame = remoteDataFrame.filter(_ => true).asInstanceOf[RemoteDataFrameProxy]
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    filteredDataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    val expectedOutput = source.getLines().toSeq.tail.mkString("\n") + "\n"


    assertTrue(filteredDataFrame.operation.isInstanceOf[FilterOp], "Filter should create FilterOp")
    assertEquals(expectedOutput, actualOutput, "Filtered data must match")
  }

  @Test
  def testSelectOperationChain(): Unit = {
    val columns = Seq("id", "value")
    // 覆盖 select 方法
    val selectedDataFrame = remoteDataFrame.select(columns: _*).asInstanceOf[RemoteDataFrameProxy]
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    selectedDataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    val expectedOutput = source.getLines().toSeq.tail.mkString("\n") + "\n"

    assertTrue(selectedDataFrame.operation.isInstanceOf[SelectOp], "Select should create SelectOp")
    val selectOp = selectedDataFrame.operation.asInstanceOf[SelectOp]
    assertEquals(columns.toList, selectOp.columns.toList, "Selected columns must match")
    assertEquals(expectedOutput, actualOutput, "Selected data must match")
  }

  @Test
  def testLimitOperationChain(): Unit = {
    val limitN = 50
    // 覆盖 limit 方法
    val limitedDataFrame = remoteDataFrame.limit(limitN).asInstanceOf[RemoteDataFrameProxy]
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    limitedDataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    val expectedOutput = source.getLines().take(limitN + 1).toSeq.tail.mkString("\n") + "\n"


    assertTrue(limitedDataFrame.operation.isInstanceOf[LimitOp], "Limit should create LimitOp")
    val limitOp = limitedDataFrame.operation.asInstanceOf[LimitOp]
    assertEquals(limitN, limitOp.n, "Limit value must match")
    assertEquals(expectedOutput, actualOutput, "Limited data must match")
  }

  @Test
  def testMapOperationChain(): Unit = {
    // 覆盖 map 方法
    val mappedDataFrame = remoteDataFrame.map(r => r).asInstanceOf[RemoteDataFrameProxy]
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    mappedDataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    val expectedOutput = source.getLines().toSeq.tail.mkString("\n") + "\n"

    assertTrue(mappedDataFrame.operation.isInstanceOf[MapOp], "Map should create MapOp")
    assertEquals(expectedOutput, actualOutput, "Mapped data must match")
  }

  @Test
  def testOperationChaining(): Unit = {
    // 验证操作链的顺序和结构
    val chainedDataFrame = remoteDataFrame.select("id", "value").filter(_ => true).limit(10).asInstanceOf[RemoteDataFrameProxy]
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    chainedDataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    val expectedOutput = source.getLines().take(11).toSeq.tail.mkString("\n") + "\n"

    assertTrue(chainedDataFrame.operation.isInstanceOf[LimitOp], "Final op should be LimitOp")
    assertEquals(expectedOutput, actualOutput, "Chain operated data must match")
  }

  @Test
  def testCollectLazyLoading(): Unit = {
    // 覆盖 private def records() 和 collect()
    val records = remoteDataFrame.collect()

    assertEquals(size, records.size, "Collect should retrieve all mock data")
  }

  @Test
  def testForeachLazyLoading(): Unit = {
    // 覆盖 foreach()
    var count = 0
    remoteDataFrame.foreach(_ => count += 1)

    assertEquals(size, count, "Foreach should iterate over all mock data")
  }

  @Test
  def testMapIteratorLazyLoading(): Unit = {
    // 覆盖 mapIterator[T]()
    val result: Int = remoteDataFrame.mapIterator(iter => iter.toIterator.size)

    assertEquals(size, result, "MapIterator should return the size of the mock data")
  }
}