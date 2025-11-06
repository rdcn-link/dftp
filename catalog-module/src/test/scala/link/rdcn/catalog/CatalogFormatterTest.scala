/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 11:00
 * @Modified By:
 */
package link.rdcn.catalog

import link.rdcn.catalog.ConfigKeys.{FAIRD_HOST_PORT, FAIRD_HOST_POSITION}
import link.rdcn.server.ServerContext
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, StructType}
import org.json.{JSONArray, JSONObject}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test


class CatalogFormatterTest {
  val mockStatistics = new DataFrameStatistics{

    override def rowCount: Long = 1234L

    override def byteSize: Long = 5678L
  }

  val mockDoc = new DataFrameDocument {

    override def getSchemaURL(): Option[String] = ???

    override def getDataFrameTitle(): Option[String] = ???

    override def getColumnURL(colName: String): Option[String] = ???

    override def getColumnAlias(colName: String): Option[String] = ???

    override def getColumnTitle(colName: String): Option[String] = ???
  }

  /**
   * 测试 getDataFrameStatisticsString 方法
   */
  @Test
  def testDataFrameStatisticsString(): Unit = {

    val jsonString = CatalogFormatter.getDataFrameStatisticsString(mockStatistics)
    assertNotNull(jsonString, "返回的 JSON 字符串不应为 null")

    // 解析 JSON 以验证内容
    val jo = new JSONObject(jsonString)

    assertEquals(1234L, jo.getLong("rowCount"), "rowCount 值不匹配")
    assertEquals(5678L, jo.getLong("byteSize"), "byteSize 值不匹配")
  }

  /**
   * 测试 getHostInfoString 方法
   */
  @Test
  def testHostInfoString(): Unit = {
    val mockContext = new MockServerContext()
    val jsonString = CatalogFormatter.getHostInfoString(mockContext)
    assertNotNull(jsonString, "返回的 JSON 字符串不应为 null")

    // 解析 JSON 以验证内容
    val jo = new JSONObject(jsonString)

    assertEquals("mock-host", jo.getString(FAIRD_HOST_POSITION), "主机位置 (position) 不匹配")
    assertEquals("9999", jo.getString(FAIRD_HOST_PORT), "主机端口 (port) 不匹配")
  }

  /**
   * 测试 getResourceStatusString 方法
   * 这将读取*实时*系统数据，因此我们只测试格式和键的存在性
   */
  @Test
  def testResourceStatusString(): Unit = {
    val resourceMap = CatalogFormatter.getResourceStatusString()
    assertNotNull(resourceMap, "返回的 Map 不应为 null")

    assertTrue(resourceMap.contains("cpu.cores"), "应包含 'cpu.cores' 键")
    assertTrue(resourceMap.contains("cpu.usage.percent"), "应包含 'cpu.usage.percent' 键")
    assertTrue(resourceMap.contains("jvm.memory.max.mb"), "应包含 'jvm.memory.max.mb' 键")
    assertTrue(resourceMap.contains("system.memory.total.mb"), "应包含 'system.memory.total.mb' 键")

    // 验证值的格式
    assertTrue(resourceMap("cpu.usage.percent").endsWith("%"), "CPU 使用率应以 '%' 结尾")
    assertTrue(resourceMap("jvm.memory.max.mb").endsWith(" MB"), "JVM 内存应以 ' MB' 结尾")
  }

  /**
   * 测试 getHostResourceString 方法
   * 这将读取*实时*系统数据，因此我们只测试格式和键的存在性
   */
  @Test
  def testHostResourceString(): Unit = {
    val jsonString = CatalogFormatter.getHostResourceString()
    assertNotNull(jsonString, "返回的 JSON 字符串不应为 null")

    val jo = new JSONObject(jsonString)

    assertTrue(jo.has("cpu.cores"), "应包含 'cpu.cores' 键")
    assertTrue(jo.has("jvm.memory.used.mb"), "应包含 'jvm.memory.used.mb' 键")

    // 验证值是否为有效数字（或带 %/MB）
    assertTrue(jo.getString("cpu.cores").toInt > 0, "CPU 核心数应大于 0")
  }

  /**
   * 测试 getDataFrameDocumentJsonString - 当 schema 为 None 时
   * 这是 CatalogFormatter.scala 中可测试的路径之一
   */
  @Test
  def testDataFrameDocumentJsonString_SchemaNone(): Unit = {
    // DataFrameDocument 的内容无关紧要，因为 schema 为 None

    val jsonString = CatalogFormatter.getDataFrameDocumentJsonString(mockDoc, None)

    assertEquals("[]", jsonString, "当 schema 为 None 时，应返回一个空的 JSON 数组")
  }

  /**
   * 测试 getDataFrameDocumentJsonString - 当 schema 为 Some(StructType.empty) 时
   * 这是 CatalogFormatter.scala 中可测试的路径之一
   */
  @Test
  def testDataFrameDocumentJsonString_SchemaEmpty(): Unit = {
    val emptySchema = Some(StructType.empty)

    val jsonString = CatalogFormatter.getDataFrameDocumentJsonString(mockDoc, emptySchema)

    assertEquals("[]", jsonString, "当 schema 为空时，应返回一个空的 JSON 数组")
  }

}