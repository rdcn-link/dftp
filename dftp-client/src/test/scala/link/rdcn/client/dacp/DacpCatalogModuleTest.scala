/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 10:36
 * @Modified By:
 */
package link.rdcn.client.dacp

import link.rdcn.catalog._
import link.rdcn.client.DftpClient
import link.rdcn.client.DftpClientTest.baseUrl
import link.rdcn.client.dacp.DacpCatalogModuleTest.{catalogService, client}
import link.rdcn.client.dacp.DacpClientTest.authenticationService
import link.rdcn.server.module.{AuthModule, BaseDftpModule}
import link.rdcn.server.{DftpServer, DftpServerConfig, ServerContext}
import link.rdcn.struct.ValueType.{IntType, StringType}
import link.rdcn.struct._
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}

import java.io.{ByteArrayInputStream, StringWriter}
import java.nio.charset.StandardCharsets

object DacpCatalogModuleTest {
  private var server: DftpServer = _
  private var client: DftpClient = _
  private val testPort = 3101 // 为此测试使用一个唯一的端口
  private val baseUrl = s"dftp://0.0.0.0:$testPort"
  private val catalogService = new CatalogService {
    override def getDataSetMetaData(datasetName: String, model: Model): Unit =
      model.add(MockCatalogData.getMockModel)

    override def getDataFrameMetaData(dataFrameName: String, model: Model): Unit =
      model.add(MockCatalogData.getMockModel)

    override def getDocument(dataFrameName: String): DataFrameDocument =
      if (dataFrameName == "my_table") MockCatalogData.mockDoc else throw new NoSuchElementException("Doc not found")

    override def getStatistics(dataFrameName: String): DataFrameStatistics =
      if (dataFrameName == "my_table") MockCatalogData.mockStats else throw new NoSuchElementException("Stats not found")

    override def getSchema(dataFrameName: String): Option[StructType] =
      if (dataFrameName == "my_table") Some(MockCatalogData.mockSchema) else None

    override def getDataFrameTitle(dataFrameName: String): Option[String] =
      if (dataFrameName == "my_table") Some(MockCatalogData.mockTitle) else None

    override def accepts(request: CatalogServiceRequest): Boolean = true

    /**
     * 列出所有数据集名称
     *
     * @return java.util.List[String]
     */
    override def listDataSetNames(): List[String] = List("my_set")

    /**
     * 列出指定数据集下的所有数据帧名称
     *
     * @param dataSetId 数据集 ID
     * @return java.util.List[String]
     */
    override def listDataFrameNames(dataSetId: String): List[String] = List("my_table")
  }

  @BeforeAll
  def startServer(): Unit = {
    val config = DftpServerConfig("0.0.0.0", testPort, dftpHome = Some("data"))
    val modules = Array(
      new BaseDftpModule,
      new DacpCatalogModule,
      new CatalogServiceModule(catalogService),
      new AuthModule(authenticationService),
    )
    server = DftpServer.start(config, modules)

    // 等待服务器启动
    Thread.sleep(2000)

    client = DftpClient.connect(baseUrl)
    assertNotNull(client, "客户端连接失败")
  }

  @AfterAll
  def stopServer(): Unit = {
    if (server != null) server.close()
  }
}

/**
 * DacpCatalogModule 的综合测试套件
 */
class DacpCatalogModuleTest {

  @Test
  def testGetSchemaAction(): Unit = {
    val actionName = "/getSchema/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString = MockCatalogData.mockSchema.toString

    assertEquals(expectedString, resultString, s"Action $actionName 返回的 Schema 字符串不匹配")
  }

  @Test
  def testGetDataFrameTitleAction(): Unit = {
    val actionName = "/getDataFrameTitle/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString = MockCatalogData.mockTitle

    assertEquals(expectedString, resultString, s"Action $actionName 返回的 Title 字符串不匹配")
  }

  @Test
  def testGetStatisticsAction(): Unit = {
    val actionName = "/getStatistics/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString =  CatalogFormatter.getDataFrameStatisticsString(MockCatalogData.mockStats)

    assertEquals(expectedString, resultString, s"Action $actionName 返回的 Statistics JSON 不匹配")
  }

  @Test
  def testGetDocumentAction(): Unit = {
    val actionName = "/getDocument/my_table"
    val resultBytes = client.doAction(actionName)
    val resultString = new String(resultBytes, StandardCharsets.UTF_8)

    val expectedString = CatalogFormatter.getDataFrameDocumentJsonString(
      MockCatalogData.mockDoc,
      Some(MockCatalogData.mockSchema)
    )

    assertEquals(expectedString, resultString, s"Action $actionName 返回的 Document JSON 不匹配")
  }

  @Test
  def testGetDataFrameMetaDataAction(): Unit = {
    val actionName = "/getDataFrameMetaData/my_table"
    val resultBytes = client.doAction(actionName)

    // 验证 RDF/XML 内容是否同构 (isomorphic)
    val expectedModel = MockCatalogData.getMockModel
    val resultModel = ModelFactory.createDefaultModel()
    resultModel.read(new ByteArrayInputStream(resultBytes), null, "RDF/XML")

    assertTrue(expectedModel.isIsomorphicWith(resultModel), s"Action $actionName 返回的 RDF/XML 内容不匹配")
  }

  @Test
  def testGetDataSetMetaDataAction(): Unit = {
    val actionName = "/getDataSetMetaData/my_set"
    val resultBytes = client.doAction(actionName)

    val expectedModel = MockCatalogData.getMockModel
    val resultModel = ModelFactory.createDefaultModel()
    resultModel.read(new ByteArrayInputStream(resultBytes), null, "RDF/XML")

    assertTrue(expectedModel.isIsomorphicWith(resultModel), s"Action $actionName 返回的 RDF/XML 内容不匹配")
  }

  @Test
  def testUnknownAction(): Unit = {
    val actionName = "/unknown/action"
    val ex = assertThrows(classOf[FlightRuntimeException], () => {
      client.doAction(actionName)
      ()
    }, "调用未知 Action 应抛出 FlightRuntimeException")

    assertTrue(ex.getMessage.contains(s"unknown action: $actionName"), "异常消息应确认未知的 Action")
  }

  // --- GetStreamHandler API 测试 ---

  @Test
  def testListDataSetsStream(): Unit = {
    val path = s"$baseUrl/listDataSets"
    val df = client.get(path)
    val expectedDF = catalogService.doListDataSets("")
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema.toString, df.schema.toString, s"GetStream $path 返回的 Schema 不匹配")
    assertEquals(expectedRows.length, actualRows.length, s"GetStream $path 返回的行数不匹配")
    assertEquals(expectedRows.head._1, actualRows.head._1, s"GetStream $path 返回的内容不匹配")
  }

  @Test
  def testListDataFramesStream(): Unit = {
    val path = s"$baseUrl/listDataFrames/my_set"
    val df = client.get(path)
    val expectedDF = catalogService.doListDataFrames("my_set",baseUrl)
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema, df.schema, s"GetStream $path 返回的 Schema 不匹配")
    assertEquals(expectedRows.length, actualRows.length, s"GetStream $path 返回的行数不匹配")
    assertEquals(expectedRows.head._1, actualRows.head._1, s"GetStream $path 返回的内容不匹配")
  }

  @Test
  def testListHostsStream(): Unit = {
    val path = s"$baseUrl/listHosts"
    val df = client.get(path)
    val expectedDF = catalogService.doListHostInfo(new MockServerContext)
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema, df.schema, s"GetStream $path 返回的 Schema 不匹配")
    assertEquals(expectedRows.length, actualRows.length, s"GetStream $path 返回的行数不匹配")
    assertEquals(expectedRows.head._1, actualRows.head._1, s"GetStream $path 返回的内容不匹配")
  }

  @Test
  @Disabled("需要实现多个Holder的请求传递")
  def testStreamHandlerChaining(): Unit = {
    // 这个测试验证 DacpCatalogModule 是否正确地将请求传递给了 "old" handler
    val path = "/oldStream"
    val df = client.get(path)
    val expectedDF = DefaultDataFrame(StructType.empty, Iterator.empty)
    val actualRows = df.collect()
    val expectedRows = expectedDF.collect()

    assertEquals(expectedDF.schema, df.schema, "GetStream 链式调用返回的 Schema 不匹配")
    assertEquals(expectedRows.head._1, actualRows.head._1, "GetStream 链式调用返回的内容不匹配")
  }

  @Test
  def testUnknownStream(): Unit = {
    val path = "/unknown/stream"

    val ex = assertThrows(classOf[FlightRuntimeException], () => {
      client.get(s"${baseUrl}${path}").collect() // 必须调用 .collect() 来触发流
      ()
    }, "请求未知 Stream 应抛出 FlightRuntimeException")

    assertTrue(ex.getMessage.contains(s"not found"), "异常消息应确认stream未知")
  }
}

/**
 * 模拟 CatalogService 返回的常量数据
 */
object MockCatalogData {
  val mockSchema: StructType = StructType.empty.add("id", IntType).add("name", StringType)
  val mockTitle: String = "My Mock Table"
  val mockStats: DataFrameStatistics = new DataFrameStatistics {

    override def rowCount: Long = 123L

    override def byteSize: Long = 456L
  }
  val mockDoc: DataFrameDocument = new DataFrameDocument() {
    override def getSchemaURL(): Option[String] = Some("SchemaURL")

    override def getDataFrameTitle(): Option[String] = Some("DataFrameTitle")

    override def getColumnURL(colName: String): Option[String] = Some("ColumnURL")

    override def getColumnAlias(colName: String): Option[String] = Some("ColumnAlias")

    override def getColumnTitle(colName: String): Option[String] = Some("ColumnTitle")
  }

  val mockListDataSetDF: DataFrame = DefaultDataFrame(
    StructType.empty.add("datasetName", StringType).add("datasetUrl", StringType),
    Seq(Row("TestSet", "dftp://host/TestSet")).iterator
  )
  val mockListDataFrameDF: DataFrame = DefaultDataFrame(
    StructType.empty.add("dataFrameName", StringType).add("dataFrameUrl", StringType),
    Seq(Row("my_table", "dftp://host/TestSet/my_table")).iterator
  )
  val mockListHostDF: DataFrame = DefaultDataFrame(
    StructType.empty.add("hostName", StringType).add("hostUrl", StringType),
    Seq(Row("localhost", "dftp://localhost:3105")).iterator
  )

  // 用于模拟 RDF/XML 响应
  def getMockModel: Model = {
    val model = ModelFactory.createDefaultModel()
    val resource = model.createResource("http://example.org/mock")
    val property = model.createProperty("http://example.org/prop#value")
    resource.addProperty(property, "MockValue")
    model
  }

  def getModelAsRdfXmlString(model: Model): String = {
    val writer = new StringWriter()
    model.write(writer, "RDF/XML")
    writer.toString
  }
}

class MockServerContext extends ServerContext {
  override def getHost(): String = "0.0.0.0"
  override def getPort(): Int = 3101
  override def getProtocolScheme(): String = "dftp"
  override def getDftpHome(): Option[String] = None
}

