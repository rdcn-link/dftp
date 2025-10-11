/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:53
 * @Modified By:
 */
package link.rdcn.struct

import link.rdcn.CommonTestBase.DataFrameInfo
import link.rdcn.CommonTestProvider
import link.rdcn.CommonTestProvider.{csvDir, dataProvider, totalLines}
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.Paths
import scala.io.Source

class DataStreamSourceTest extends CommonTestProvider {

  @Test
  def testCsvWithHeader(): Unit = {
    val csvDataFrameInfo: DataFrameInfo = dataProvider.getDataFrameInfo("/csv/data_1.csv").getOrElse(null)
    val mockFile = new File(csvDataFrameInfo.path)
    val csvSource = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString)
    val expectedOutput = csvSource.getLines().toSeq.tail.toList.head

    // 覆盖 csv 方法
    val source = DataStreamSource.csv(mockFile, delimiter = ",")

    // 验证返回的 DataStreamSource 属性
    assertEquals(10001L, source.rowCount, "rowCount must match mock countLinesFast result")
    assertEquals(StructType.empty.add("id", LongType).add("value", DoubleType), source.schema, "Schema must match mock inferSchema result")

    // 验证迭代器：它应该包含被跳过的 sample/header 行，并返回转换后的类型化 Row
    val iter = source.iterator
    val rows = iter.toList
    assertEquals(expectedOutput, rows.toSeq.map(row=>s"${row._1},${row._2}").head, "Data must match mock data result")
    csvSource.close()
  }

  @Test
  def testExcel(): Unit = {
    val excelDataFrameInfo: DataFrameInfo = dataProvider.getDataFrameInfo("/excel/data_1.xlsx").getOrElse(null)
    val file = new File(excelDataFrameInfo.path)
    val excelPath = file.getPath

    // 覆盖 excel 方法
    val source = DataStreamSource.excel(excelPath)

    // 验证返回的 DataStreamSource 属性
    assertEquals(-1L, source.rowCount, "rowCount must be -1")

    // 验证迭代器
    val iter = source.iterator
    val rows = iter.toList
    assertEquals(totalLines, rows.size, "Iterator should contain 0 mock excel rows")
  }

  @Test
  def testFilePathNonRecursive(): Unit = {
    val fileDataFrameInfo: DataFrameInfo = dataProvider.getDataFrameInfo("/bin").getOrElse(null)
    val mockDir = new File(fileDataFrameInfo.path)

    // 覆盖 filePath(dir, false)
    val source = DataStreamSource.filePath(mockDir, recursive = false)

    // 验证返回的 DataStreamSource 属性
    assertEquals(-1L, source.rowCount, "rowCount must be -1")
    assertEquals(StructType.binaryStructType, source.schema, "Schema must be binaryStructType")

    // 验证迭代器内容 (Row.fromTuple(_))
    val iter = source.iterator
    val row = iter.next()
    assertEquals(7, row.values.size, "Row must contain 7 file attributes and Blob")
  }
}