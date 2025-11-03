package link.rdcn.server


import link.rdcn.client.DftpClient
import link.rdcn.server.TestDataGenerator.getOutputDir
import link.rdcn.server.module.{BaseDftpModule, DirectoryDataSourceModule, RequireAuthenticatorEvent}
import link.rdcn.struct.StructType
import link.rdcn.struct.ValueType.StringType
import link.rdcn.user
import link.rdcn.user._
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.File
import java.nio.file.Paths


/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 19:13
 * @Modified By:
 */

// --- 测试服务器启动与关闭 ---
object DftpServerTest {

  var server: DftpServer = _
  val host = "0.0.0.0"
  val port = 3101
  val baseUrl = s"dftp://$host:$port"

  val resourceUrl = getClass.getProtectionDomain.getCodeSource.getLocation
  val testClassesDir = new File(resourceUrl.toURI)
  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = Paths.get(baseDir, "bin").toString
  val csvDir = Paths.get(baseDir, "csv").toString

  //必须在DfInfos前执行一次
  TestDataGenerator.generateTestData(binDir, csvDir, baseDir)

  @BeforeAll
  def startServer(): Unit = {
    val directoryDataSourceModule = new DirectoryDataSourceModule
    directoryDataSourceModule.setRootDirectory(new File(baseDir))
    val modules = Array(directoryDataSourceModule, new BaseDftpModule, new MockAuthModule)
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3101, Some("data")), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    if (server != null) {
      server.close()
    }
  }
}


class DftpServerTest {

  // --- 客户端连接与认证测试 ---

  @Test
  def clientConnectAndLoginAnonymousTest(): Unit = {
    // 匿名登录 (默认)
    val client = DftpClient.connect(DftpServerTest.baseUrl)
    assertNotNull(client, "匿名连接客户端不应为空")
  }

  @Test
  def clientConnectAndLoginSuccessTest(): Unit = {
    // 使用特定凭证成功登录
    val validCreds = UsernamePassword("test_user", "test_pass")
    val client = DftpClient.connect(DftpServerTest.baseUrl, validCreds)
    assertNotNull(client, "有效凭证连接客户端不应为空")
  }

//  @Test
//  def clientConnectAndLoginFailedTest(): Unit = {
//    // 使用无效凭证登录，根据 DftpClient 的实现，它会在 login 时抛出 FlightRuntimeException
//    val invalidCreds = UsernamePassword("wrong_user", "wrong_pass")
//
//    val exception = assertThrows(classOf[FlightRuntimeException], () => {
//      DftpClient.connect(DftpServerTest.baseUrl, invalidCreds)
//      ()
//    }, "无效凭证连接应抛出 FlightRuntimeException")
//
//    assertTrue(exception.getMessage.contains("UNAUTHENTICATED"), "异常消息应包含 'UNAUTHENTICATED'")
//  }

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
    val client = DftpClient.connect(DftpServerTest.baseUrl)
    // 使用完整 URL 获取
    val df = client.get(s"${DftpServerTest.baseUrl}/csv/data_1.csv")

    // 验证 Schema
    val expectedSchema = StructType.empty.add("id", StringType).add("value", StringType)
    assertEquals(expectedSchema, df.schema, "DataFrame 的 Schema 不匹配")

    // 验证数据
    val rows = df.collect()
    assertEquals(10001, rows.length, "DataFrame 的行数不匹配")
  }




  @Test
  def getStreamNotFoundTest(): Unit = {
    val client = DftpClient.connect(DftpServerTest.baseUrl)

    // 获取不存在的数据源，服务器应返回 404 NOT_FOUND，客户端捕获为 FlightRuntimeException
    val exception = assertThrows(classOf[FlightRuntimeException], () => {
      client.get(s"${DftpServerTest.baseUrl}/non_existent_data").collect()
      ()
    }, "获取不存在的数据应抛出 FlightRuntimeException")

    assertTrue(exception.getMessage.contains("There was an error servicing your request."), "异常消息应包含模拟模块的错误信息")
  }

  @Test
  def getStreamInvalidClientUrlTest(): Unit = {
    val client = DftpClient.connect(DftpServerTest.baseUrl)

    // 客户端会检查 URL 的 host 和 port
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      client.get("dftp://another.host:1234/data")
      ()
    }, "使用不匹配的 host/port 调用 get 应抛出 IllegalArgumentException")

    assertTrue(exception.getMessage.contains("Invalid request URL"), "异常消息应指示 URL 无效")

  }


//  // --- PutStream (client.put) 测试 ---
//
//  @Test
//  def putTestEmpty(): Unit = {
//    val client = DftpClient.connect(DftpServerTest.baseUrl)
//    val result: Array[Byte] = client.put(DataFrame.empty())
//
//    val resultJson = new JSONObject(CodecUtils.decodeString(result))
//    assertEquals("success", resultJson.getString("status"), "Put 空 DataFrame 的状态不为 'success'")
//    assertEquals(0, resultJson.getInt("rows_received"), "Put 空 DataFrame 接收到的行数应为 0")
//
//  }
//
//  @Test
//  def putTestWithData(): Unit = {
//    val client = DftpClient.connect(DftpServerTest.baseUrl)
//
//    val schema = StructType.empty.add("id", LongType).add("name", StringType)
//    val rows = Seq(
//      Row(1L, "Alice"),
//      Row(2L, "Bob")
//    ).toIterator
//    val df = DefaultDataFrame(schema, rows)
//
//    val result: Array[Byte] = client.put(df)
//
//    val resultJson = new JSONObject(CodecUtils.decodeString(result))
//    assertEquals("success", resultJson.getString("status"), "Put 2行数据 的状态不为 'success'")
//    assertEquals(2L, resultJson.getInt("rows_received"), "Put 2行数据 接收到的行数应为 2")
//
//  }
}

// --- 模拟认证模块 ---
class MockAuthModule extends DftpModule{

  private val authenticationService = new AuthenticationService {
    override def accepts(request: AuthenticationRequest): Boolean = true

    override def authenticate(credentials: user.Credentials): user.UserPrincipal =
      UserPrincipalWithCredentials(credentials)
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = event.isInstanceOf[RequireAuthenticatorEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireAuthenticatorEvent => r.holder.set(authenticationService)
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}
}

