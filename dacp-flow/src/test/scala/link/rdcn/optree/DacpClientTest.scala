package link.rdcn.optree

import link.rdcn.cook.{DacpClient, DacpCookModule}
import link.rdcn.recipe.{ExecutionResult, Flow, SourceNode, Transformer11}
import link.rdcn.server.module.{BaseDftpDataSource, BaseDftpModule, DirectoryDataSourceModule}
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, DftpServer, DftpServerConfig, EventSourceService, ServerContext}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}
import link.rdcn.util.CodecUtils
import org.json.JSONObject
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

class DataSourceModule extends DftpModule{

  private val eventSourceService = new EventSourceService {
    override def create(): CrossModuleEvent = new BaseDftpDataSource {
      override def getDataFrame(dataFrameName: String): DataFrame = {
        val structType = StructType.empty.add("col1", StringType)
        val rows = Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator
        DefaultDataFrame(structType, rows)
      }

      override def action(actionName: String, parameter: Array[Byte]): Array[Byte] = {
        CodecUtils.encodeString(new JSONObject().put("status","success").toString())
      }

      override def put(dataFrame: DataFrame): Array[Byte] = {
        CodecUtils.encodeString(new JSONObject().put("status","success").toString())
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(eventSourceService)
  }

  override def destroy(): Unit = {}
}

class AuthModule extends DftpModule{

  private val authenticationService = new AuthenticationService {
    override def accepts(credentials: Credentials): Boolean = true

    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(authenticationService)

  override def destroy(): Unit = {}
}


object DacpClientTest{

  var server: DftpServer = _

  @BeforeAll
  def startServer(): Unit = {
    val modules = Array(new DataSourceModule, new BaseDftpModule, new AuthModule, new DacpCookModule)
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3102, Some("data"), false, "dacp"), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    server.close()
  }

}

class DacpClientTest {

  @Test
  def getTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102")
    val df = dc.get("dacp://0.0.0.0:3102/abc")
    df.foreach(println)
  }

  @Test
  def cookTest(): Unit = {

    val dc = DacpClient.connect("dacp://0.0.0.0:3102")

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
