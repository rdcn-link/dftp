package link.rdcn.client

<<<<<<<< HEAD:cook-module/src/test/scala/link/rdcn/client/DacpClientTest.scala
import link.rdcn.catalog.{DacpCatalogModule, DataProvider, DataProviderModule}
========
import link.rdcn.catalog.{CatalogService, CatalogServiceModule, CatalogServiceRequest, DacpCatalogModule}
import link.rdcn.client.DacpClient
>>>>>>>> origin/main:dftp-client/src/test/scala/link/rdcn/client/DacpClientTest.scala
import link.rdcn.cook.DacpCookModule
import link.rdcn.recipe.{ExecutionResult, Flow, SourceNode, Transformer11}
import link.rdcn.server.ServerContext
import link.rdcn.server.module.{AuthModule, BaseDftpModule, DataFrameProviderModule, DataFrameProviderService}
import link.rdcn.server.{DftpServer, DftpServerConfig}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._
import link.rdcn.user._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

object DacpClientTest{

  var server: DftpServer = _
  val catalogService = new CatalogService {

    override def listDataSetNames(): List[String] = List("DataSet")

    override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
      val model = ModelFactory.createDefaultModel()

      val ns = "http://example.org/data/"
      model.setNsPrefix("ex", ns)

      val datasetResource = model.createResource(s"${ns}dataset/$dataSetId")
      val personResource = model.createResource(s"${ns}person/123")

      datasetResource.addProperty(model.createProperty(ns, "hasName"), "示例数据集")
        .addProperty(model.createProperty(ns, "createdBy"), personResource)

      personResource.addProperty(RDF.`type`, model.createResource(s"${ns}Person"))
        .addProperty(model.createProperty(ns, "name"), "张三")

      rdfModel.add(model)
    }

    override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
      val model = ModelFactory.createDefaultModel()

      val ns = "http://example.org/data/"
      model.setNsPrefix("ex", ns)

      val datasetResource = model.createResource(s"${ns}DataFrameName/$dataFrameName")
      val personResource = model.createResource(s"${ns}person/123")

      datasetResource.addProperty(model.createProperty(ns, "hasName"), "示例数据集")
        .addProperty(model.createProperty(ns, "createdBy"), personResource)

      personResource.addProperty(RDF.`type`, model.createResource(s"${ns}Person"))
        .addProperty(model.createProperty(ns, "name"), "张三")

      rdfModel.add(model)
    }

    override def listDataFrameNames(dataSetId: String): List[String] = List("DataFrame")

    override def getDocument(dataFrameName: String): DataFrameDocument = {
      new DataFrameDocument {
        override def getSchemaURL(): Option[String] = Some(s"$dataFrameName url")

        override def getDataFrameTitle(): Option[String] = Some(s"$dataFrameName title")

        override def getColumnURL(colName: String): Option[String] = Some(s"$dataFrameName $colName url")

        override def getColumnAlias(colName: String): Option[String] = Some(s"$dataFrameName $colName alias")

        override def getColumnTitle(colName: String): Option[String] = Some(s"$dataFrameName $colName title")
      }
    }

    override def getStatistics(dataFrameName: String): DataFrameStatistics = new DataFrameStatistics {
      override def rowCount: Long = -1L

      override def byteSize: Long = -1L
    }

    override def getSchema(dataFrameName: String): Option[StructType] =
      Some(StructType.empty.add("col1", StringType))

    override def getDataFrameTitle(dataFrameName: String): Option[String] = Some(dataFrameName)

    override def accepts(request: CatalogServiceRequest): Boolean = true
  }
  val permissionService = new PermissionService {
    override def accepts(user: UserPrincipal): Boolean = true

    /**
     * 判断用户是否具有某项权限
     *
     * @param user          已认证用户
     * @param dataFrameName 数据帧名称
     * @param opList        操作类型列表（Java List）
     * @return 是否有权限
     */
    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean =
      user.asInstanceOf[UserPrincipalWithCredentials].credentials match {
        case Credentials.ANONYMOUS => false
        case UsernamePassword(username, password) =>true
      }
  }

  val dataFrameProviderService = new DataFrameProviderService {
    override def accepts(dataFrameUrl: String): Boolean = true

    override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame = {
      new DataStreamSource {
        override def rowCount: Long = -1L

        override def schema: StructType = StructType.empty.add("col1", StringType)

        override def iterator: ClosableIterator[Row] =
          ClosableIterator(Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator)()
      }.dataFrame
    }
  }

  val authenticationService = new AuthenticationService {
    override def accepts(credentials: Credentials): Boolean = true

    /**
     * 用户认证，成功返回认证后的保持用户登录状态的凭证
     */
    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  @BeforeAll
  def startServer(): Unit = {
    val modules = Array(
      new BaseDftpModule,
      new DacpCookModule,
      new DacpCatalogModule,
      new DataFrameProviderModule(dataFrameProviderService),
      new CatalogServiceModule(catalogService),
      new AuthModule(authenticationService),
      new PermissionServiceModule(permissionService)
    )
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3102).withProtocolScheme("dacp"), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    server.close()
  }

}

class DacpClientTest {

  @Test
  def catalogModuleTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102")
    println("数据帧列表-----------------")
    dc.listDataFrameNames("/abc").foreach(println)
    println("数据集列表-----------------")
    dc.listDataSetNames().foreach(println)
    println("数据帧元数据信息-----------------")
    println(dc.getDataFrameMetaData("/abc"))
    println("数据集元数据信息-----------------")
    println(dc.getDataSetMetaData("ds"))
    println("getSchema-------------")
    println(dc.getSchema("/abc"))

    println(dc.getDataFrameSize("/abc"))
    println(dc.getDocument("/abc"))
    println(dc.getStatistics("/abc"))

  }

  @Test
  def getTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102", UsernamePassword("admin", "admin"))
    val df = dc.get("dacp://0.0.0.0:3102/abcd")
    df.foreach(println)
  }

  @Test
  def cookTest(): Unit = {

    val dc = DacpClient.connect("dacp://0.0.0.0:3102", UsernamePassword("admin", "admin"))

    val udf = new Transformer11 {
      override def transform(dataFrame: DataFrame): DataFrame = {
        dataFrame.limit(5)
      }
    }

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/abc"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: ExecutionResult = dc.cook(transformerDAG)
    dfs.single().foreach(println)
  }
}
