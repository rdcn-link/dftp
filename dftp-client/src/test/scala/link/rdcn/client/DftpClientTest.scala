package link.rdcn.client

import link.rdcn.DftpClientTestBase.getLine
import link.rdcn.DftpClientTestProvider.{baseDir, binDir, csvDir}
import link.rdcn.client.DftpClientTest.baseUrl
import link.rdcn.server.module.{AuthModule, BaseDftpModule, DirectoryDataSourceModule}
import link.rdcn.server.{DftpServer, DftpServerConfig}
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.struct._
import link.rdcn.user._
import link.rdcn.util.CodecUtils
import link.rdcn.{ActionModule, DftpClientTestDataGenerator, PutModule, user}
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.Paths
import scala.io.{BufferedSource, Source}


/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 19:13
 * @Modified By:
 */

object DftpClientTest {

  var server: DftpServer = _
  val host = "0.0.0.0"
  val port = 3101
  val baseUrl = s"dftp://$host:$port"

  //必须在DfInfos前执行一次
  DftpClientTestDataGenerator.generateTestData(binDir, csvDir, baseDir)

  val authenticationService = new AuthenticationService {
    override def accepts(request: Credentials): Boolean = true

    override def authenticate(credentials: user.Credentials): user.UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  @BeforeAll
  def startServer(): Unit = {
    val directoryDataSourceModule = new DirectoryDataSourceModule
    directoryDataSourceModule.setRootDirectory(new File(baseDir))
    val modules = Array(directoryDataSourceModule, new BaseDftpModule, new AuthModule(authenticationService), new PutModule, new ActionModule)
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3101, Some("data")), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    if (server != null) {
      server.close()
    }
  }
}


class DftpClientTest {
  val client = DftpClient.connect(baseUrl)

  // --- 客户端连接与认证测试 ---

  @Test
  def clientConnectAndLoginAnonymousTest(): Unit = {
    // 匿名登录 (默认)
    assertNotNull(client, "匿名连接客户端不应为空")
  }

  @Test
  def clientConnectAndLoginSuccessTest(): Unit = {
    // 使用特定凭证成功登录
    val validCreds = UsernamePassword("test_user", "test_pass")
    val client = DftpClient.connect(baseUrl, validCreds)
    assertNotNull(client, "有效凭证连接客户端不应为空")
  }

  @Test
  def clientConnectInvalidUrlTest(): Unit = {
    // 测试无效的 URL 格式
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      DftpClient.connect("http://invalid_schema:9999") // DftpClient.connect 只接受 dftp://
      ()
    }, "无效的 URL 模式应抛出 IllegalArgumentException")

    assertTrue(exception.getMessage.contains("Illegal character"), "异常消息应确认 URL 无效")
  }

  // --- GetStream (client.get) 测试 ---

  @Test
  def getStreamTest(): Unit = {
    // 使用完整 URL 获取
    val df = client.get(s"${baseUrl}/csv/data_1.csv")

    // 验证 Schema
    val expectedSchema = StructType.empty.add("id", LongType).add("value", DoubleType)
    assertEquals(expectedSchema, df.schema, "DataFrame 的 Schema 不匹配")

    // 验证数据
    val rows = df.collect()
    assertEquals(10000, rows.length, "DataFrame 的行数不匹配")
  }




  @Test
  def getStreamNotFoundTest(): Unit = {
    // 获取不存在的数据源，服务器应返回 404 NOT_FOUND，客户端捕获为 FlightRuntimeException
    val exception = assertThrows(classOf[FlightRuntimeException], () => {
      client.get(s"${baseUrl}/non_existent_data").collect()
      ()
    }, "获取不存在的数据应抛出 FlightRuntimeException")

    assertTrue(exception.getMessage.contains("not found"), "异常消息应包含模拟模块的错误信息")
  }

  @Test
  def getStreamInvalidClientUrlTest(): Unit = {
    val client = DftpClient.connect(baseUrl)

    // 客户端会检查 URL 的 host 和 port
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      client.get("dftp://another.host:1234/data")
      ()
    }, "使用不匹配的 host/port 调用 get 应抛出 IllegalArgumentException")

    assertTrue(exception.getMessage.contains("Invalid request URL"), "异常消息应指示 URL 无效")

  }

  @Test
  def testGet(): Unit = {
    val source: BufferedSource = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString)
    val expectedOutput = source.getLines().toSeq.tail.mkString("\n") + "\n"
    val dataFrame = client.get("dftp://0.0.0.0:3101/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    dataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput)
    source.close()
  }


  @Test
  def testdoAction(): Unit = {
    val ackBytes: Array[Byte] = client.doAction("actionName")
    assertEquals("success", CodecUtils.decodeString(ackBytes))
  }

  @Test
  def testPut(): Unit = {
    val dataStreamSource: DataStreamSource = DataStreamSource.filePath(new File(""))
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    val batchSize = 100
    val ackBytes: Array[Byte] = client.put(dataFrame, batchSize)
    assertEquals("success", CodecUtils.decodeString(ackBytes))
  }
}

