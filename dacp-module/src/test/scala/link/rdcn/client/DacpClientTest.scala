package link.rdcn.client

import link.rdcn.catalog.{DacpCatalogModule, DataProvider, DataProviderModule}
import link.rdcn.cook.DacpCookModule
import link.rdcn.recipe.{ExecutionResult, Flow, SourceNode, Transformer11}
import link.rdcn.server.module.{BaseDftpModule, DataFrameProviderRequest}
import link.rdcn.server.{DftpServer, DftpServerConfig}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._
import link.rdcn.user._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

object DacpClientTest{

  var server: DftpServer = _
  val dataProvider = new DataProvider {

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

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = {

      new DataStreamSource {
        override def rowCount: Long = -1L

        override def schema: StructType = StructType.empty.add("col1", StringType)

        override def iterator: ClosableIterator[Row] =
          ClosableIterator(Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator)()
      }
    }

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

    override def accepts(request: DataFrameProviderRequest): Boolean = true
  }
  val authProvider = new AuthProvider {

    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
      user.asInstanceOf[UserPrincipalWithCredentials].credentials match {
        case Credentials.ANONYMOUS => false
        case UsernamePassword(username, password) =>true
      }
    }

    override def accepts(request: AuthenticationRequest): Boolean = true
  }

  @BeforeAll
  def startServer(): Unit = {
    val modules = Array(new BaseDftpModule,
      new AuthProviderModule(authProvider), new DacpCookModule,
      new DacpCatalogModule, new DataProviderModule(dataProvider))
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
    val df = dc.get("dacp://0.0.0.0:3102/abc")
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
