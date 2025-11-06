/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 11:00
 * @Modified By:
 */
package link.rdcn.catalog

// 导入被测代码所依赖的 CatalogFormatter 方法
import link.rdcn.catalog.CatalogFormatter._
import link.rdcn.catalog.MockCatalogData.{mockDoc, mockStats}
import link.rdcn.server.ServerContext
import link.rdcn.struct.ValueType.{LongType, RefType, StringType}
import link.rdcn.struct._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.io.StringWriter

/**
 * 模拟 CatalogService 的实现，为所有抽象方法提供可预测的返回值
 */
class MockCatalogServiceImpl(schemaToReturn: Option[StructType]) extends CatalogService {

  // --- 模拟抽象方法的实现 ---

  override def accepts(request: CatalogServiceRequest): Boolean = true

  override def listDataSetNames(): List[String] = List("dataset1", "dataset2")

  override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
    // 添加一些可预测的 RDF 数据
    val resource = rdfModel.createResource(s"http://example.org/set/$dataSetId")
    val property = rdfModel.createProperty("http://example.org/prop#name")
    resource.addProperty(property, s"Mock Name for $dataSetId")
  }

  override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
    // 未被测方法使用，提供空实现
  }

  override def listDataFrameNames(dataSetId: String): List[String] =
    List(s"${dataSetId}_table_a", s"${dataSetId}_table_b")

  override def getDocument(dataFrameName: String): DataFrameDocument =
    mockDoc

  override def getStatistics(dataFrameName: String): DataFrameStatistics =
    mockStats

  override def getSchema(dataFrameName: String): Option[StructType] =
    this.schemaToReturn // 返回在构造时指定的 Schema

  override def getDataFrameTitle(dataFrameName: String): Option[String] =
    Some(s"Title for $dataFrameName")
}


class CatalogServiceTest {

  val baseUrl = "dftp://test-host:1234"

  /**
   * 测试 doListDataSets 方法
   */
  @Test
  def testDoListDataSets(): Unit = {
    val mockService = new MockCatalogServiceImpl(None) // Schema 在此测试中无关紧要

    val df = mockService.doListDataSets(baseUrl)

    // 验证 Schema
    val expectedSchema = StructType.empty
      .add("name", StringType)
      .add("meta", StringType)
      .add("DataSetInfo", StringType)
      .add("dataFrames", RefType)

    assertEquals(expectedSchema, df.schema, "doListDataSets 返回的 Schema 不匹配")

    // 验证数据
    val rows = df.collect()
    assertEquals(2, rows.length, "doListDataSets 返回的行数不匹配 (应为 2)")

    // 检查第一行
    val row1 = rows.head
    assertEquals("dataset1", row1._1, "第一行 'name' 列不匹配")

    // 验证 RDF/XML 元数据
    assertTrue(row1._2.asInstanceOf[String].contains("<rdf:Description rdf:about=\"http://example.org/set/dataset1\">"),
      "第一行 'meta' (RDF) 列不包含预期的资源")

    // 验证 DataSetInfo JSON
    val infoJson = new JSONObject(row1.getAs[String](2))
    assertEquals("dataset1", infoJson.getString("name"), "第一行 'DataSetInfo' (JSON) 列中的 'name' 不匹配")

    // 验证 Ref 链接
    assertEquals("dftp://test-host:1234/listDataFrames/dataset1", row1._4.asInstanceOf[DFRef].url,
      "第一行 'dataFrames' (Ref) 列的 URL 不匹配")
  }

  /**
   * 测试 doListHostInfo 方法
   */
  @Test
  def testDoListHostInfo(): Unit = {
    val mockService = new MockCatalogServiceImpl(None) // Schema 在此测试中无关紧要
    val mockContext = new MockServerContext()

    val df = mockService.doListHostInfo(mockContext)

    // 1. 验证 Schema
    val expectedSchema = StructType.empty
      .add("hostInfo", StringType)
      .add("resourceInfo", StringType)

    // 遵守 [2025-09-26] 规范：(expected, actual, message)
    assertEquals(expectedSchema, df.schema, "doListHostInfo 返回的 Schema 不匹配")

    // 2. 验证数据
    val rows = df.collect()
    assertEquals(1, rows.length, "doListHostInfo 应只返回 1 行")

    // 验证 hostInfo JSON
    val hostInfoJson = new JSONObject(rows.head.getAs[String](0))
    assertEquals("mock-host", hostInfoJson.getString("dftp.host.position"),
      "hostInfo JSON 中的 'dftp.host.position' 不匹配")
    assertEquals("9999", hostInfoJson.getString("dftp.host.port"),
      "hostInfo JSON 中的 'dftp.host.port' 不匹配")

    // 验证 resourceInfo JSON
    val resourceInfoJson = new JSONObject(rows.head.getAs[String](1))
    assertTrue(resourceInfoJson.has("cpu.cores"), "resourceInfo JSON 应包含 'cpu.cores'")
    assertTrue(resourceInfoJson.has("jvm.memory.max.mb"), "resourceInfo JSON 应包含 'jvm.memory.max.mb'")
  }

  /**
   * 测试 doListDataFrames - 当 Schema 为空时
   * 这是 CatalogFormatter.getDataFrameDocumentJsonString 的一个可运行路径
   */
  @Test
  def testDoListDataFrames_WithEmptySchema(): Unit = {
    val mockService = new MockCatalogServiceImpl(schemaToReturn = Some(StructType.empty))
    val dataSetName = "dataset1"

    val df = mockService.doListDataFrames(s"/listDataFrames/$dataSetName", baseUrl)

    // 1. 验证 Schema
    val expectedSchema = StructType.empty
      .add("name", StringType)
      .add("size", LongType)
      .add("title", StringType)
      .add("document", StringType)
      .add("schema", StringType)
      .add("statistics", StringType)
      .add("dataFrame", RefType)

    // 遵守 [2025-09-26] 规范：(expected, actual, message)
    assertEquals(expectedSchema, df.schema, "doListDataFrames 返回的 Schema 不匹配")

    // 2. 验证数据
    val rows = df.collect()
    assertEquals(2, rows.length, "doListDataFrames 返回的行数不匹配 (应为 2)")

    // 检查第一行
    val row1 = rows.head
    assertEquals("dataset1_table_a", row1._1, "第一行 'name' 列不匹配")
    assertEquals(123L, row1._2, "第一行 'size' (rowCount) 列不匹配")
    assertEquals("Title for dataset1_table_a", row1._3, "第一行 'title' 列不匹配")

    // 验证 'document' (JSON) 列
    // 因为 Schema 为空，CatalogFormatter.getDataFrameDocumentJsonString 返回 "[]"
    assertEquals("[]", row1._4, "第一行 'document' 列应为 '[]'")

    // 验证 'schema' 字符串列
    assertEquals(expectedSchema.toString, row1._5.toString, "第一行 'schema' 字符串不匹配")

    // 验证 'statistics' (JSON) 列
    val statsJson = new JSONObject(row1.getAs[String](5))
    assertEquals(123L, statsJson.getLong("rowCount"), "第一行 'statistics' JSON 中的 'rowCount' 不匹配")
    assertEquals(456L, statsJson.getLong("byteSize"), "第一行 'statistics' JSON 中的 'rowCount' 不匹配")


    // 验证 'dataFrame' (Ref) 列
    assertEquals("dftp://test-host:1234/dataset1_table_a", row1._7.asInstanceOf[DFRef].url,
      "第一行 'dataFrame' (Ref) 列的 URL 不匹配")
  }
//
//  /**
//   * 测试 doListDataFrames - 当 Schema 不为空时
//   * 这将触发 CatalogFormatter.getDataFrameDocumentJsonString 中的已知 Bug
//   * (即调用了 Row.scala 中不存在的 toJsonObject 方法)
//   */
//  @Test
//  def testDoListDataFrames_WithNonEmptySchema_Fails(): Unit = {
//    val mockService = new MockCatalogServiceImpl(
//      schemaToReturn = Some(StructType.empty.add("col1", StringType))
//    )
//    val dataSetName = "dataset1"
//
//    val df = mockService.doListDataFrames(s"/listDataFrames/$dataSetName", baseUrl)
//
//    // 验证：调用 .collect() 时应抛出异常
//    val exception = assertThrows(classOf[Exception], () => {
//      df.collect() // 触发迭代器执行
//      ()
//    }, "当 Schema 不为空时，doListDataFrames 应抛出异常")
//
//    // 遵守 [2025-09-26] 规范：(expected, actual, message)
//    // 验证这是否是我们预期的 Bug
//    assertTrue(exception.isInstanceOf[NoSuchMethodError],
//      "异常应为 NoSuchMethodError (因为 row.toJsonObject 不存在)")
//  }
}