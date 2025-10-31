package link.rdcn.server

import link.rdcn.client.DftpClient
import link.rdcn.server.module.{BaseDftpDataSource, BaseDftpModuleTest, DirectoryDataSourceModule}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}
import link.rdcn.util.CodecUtils
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 19:13
 * @Modified By:
 */
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


object DftpServerTest{

  var server: DftpServer = _

  @BeforeAll
  def startServer(): Unit = {
    val modules = Array(new DataSourceModule, new BaseDftpModuleTest, new AuthModule)
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3101, Some("data")), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    server.close()
  }

}

class DftpServerTest {

  @Test
  def getStreamTest(): Unit = {
    val client = DftpClient.connect("dftp://0.0.0.0:3101")
    val df = client.get("dftp://0.0.0.0:3101/")
    df.foreach(println)
  }

  @Test
  def actionTest(): Unit = {
    val client = DftpClient.connect("dftp://0.0.0.0:3101")
    val result: Array[Byte] = client.doAction("test")
    val expectResult: String = new JSONObject().put("status","success").toString()
    assertEquals(expectResult, CodecUtils.decodeString(result), "result should be matched.")
  }

  @Test
  def putTest(): Unit = {
    val client = DftpClient.connect("dftp://0.0.0.0:3101")
    val result: Array[Byte] = client.put(DataFrame.empty())
    val expectResult: String = new JSONObject().put("status","success").toString()
    assertEquals(expectResult, CodecUtils.decodeString(result), "result should be matched.")
  }

}
