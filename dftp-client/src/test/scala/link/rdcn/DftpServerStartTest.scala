//package link.rdcn
//
//import link.rdcn.DftpServerStartTest.{baseUrl, testFileContent, testFileName}
//import link.rdcn.client.DftpClient
//import link.rdcn.server.module.{BaseDftpModule, FileDirectoryDataSourceModule, UserPasswordAuthModule}
//import link.rdcn.server.{DftpServer, DftpServerConfig}
//import link.rdcn.struct.ValueType.{DoubleType, LongType}
//import link.rdcn.struct.{Row, StructType}
//import link.rdcn.user._
//import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
//import org.junit.jupiter.api.io.TempDir
//import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
//
//import java.io.{File, FileInputStream, InputStreamReader}
//import java.nio.file.{Path, Paths}
//import java.util.Properties
//import scala.io.Source
//import scala.util.Failure
//
//object DftpServerStartTest {
//  private var server: DftpServer = _
//  private val serverPort = 3101 // 为本测试使用一个唯一的端口
//  private val baseUrl = s"dftp://localhost:$serverPort"
//  private val testFileName = "csv/data_1.csv"
//  private var testFileContent: Row = _
//
//
//  @TempDir
//  private var homeDir: File = _
//
//  @BeforeAll
//  def startServer(): Unit = {
//    // --- DftpServerStart 所需的模拟 dftpHome 环境 ---
//    homeDir = new File(getResourcePath(""))
//    val dftpHome = homeDir.getAbsolutePath
//    val confFile = new File(Paths.get(dftpHome, "conf", "dftp.conf").toString)
//    var source: Source = null
//    try {
//      source = Source.fromFile(Paths.get(dftpHome, "data", testFileName).toFile)
//      val lines = source.getLines()
//      val remainingLines = lines.drop(1)
//      val secondLineOption = remainingLines.next()
//
//      // 3. 如果第二行存在，则按逗号分割并转换为 Seq
//      testFileContent = Row.fromSeq(secondLineOption.split(",").toSeq)
//    } catch {
//      case ex: Exception => Failure(ex) // 捕获异常并返回 Failure
//    } finally {
//      // 4. 无论成功还是失败，都在 finally 块中关闭资源
//      if (source != null) {
//        source.close()
//      }
//    }
//
//    val props = new Properties()
//    props.load(new InputStreamReader(new FileInputStream(confFile), "UTF-8"))
//
//    val dftpServerConfig = DftpServerConfig(
//      props.getProperty("dftp.host.position"),
//      props.getProperty("dftp.host.port").toInt,
//      Some(dftpHome),
//      Some(Paths.get(dftpHome, "data").toFile.getAbsolutePath)
//    )
//
//    val directoryDataSourceModule = new FileDirectoryDataSourceModule
//    directoryDataSourceModule.setRootDirectory(new File(dftpServerConfig.dftpDataSource.getOrElse("")))
//
//    val userPasswordAuthService = new UserPasswordAuthService {
//      override def authenticate(credentials: Credentials): UserPrincipal =
//        UserPrincipalWithCredentials(credentials)
//
//      override def accepts(credentials: Credentials): Boolean = true
//    }
//
//    server = new DftpServer(dftpServerConfig) {
//      modules.addModule(new BaseDftpModule)
//        .addModule(new UserPasswordAuthModule(userPasswordAuthService))
//        .addModule(directoryDataSourceModule)
//    }
//
//    server.start()
//  }
//
//  @AfterAll
//  def stopServer(): Unit = {
//    if (server != null) {
//      server.close()
//    }
//  }
//
//  def getResourcePath(resourceName: String): String = {
//    val url = Option(getClass.getClassLoader.getResource(resourceName))
//      .orElse(Option(getClass.getResource(resourceName))) // 先到test-classes中查找，然后到classes中查找
//      .getOrElse(throw new RuntimeException())
//    val nativePath: Path = Paths.get(url.toURI())
//    nativePath.toString
//  }
//}
//
///**
// * 针对 DftpServerStart 的测试用例。
// */
//class DftpServerStartTest {
//
//  /**
//   * 通过 API 测试服务器是否已按 DftpServerStart 的配置正确启动。
//   */
//  @Test
//  def testServerStartedAndModulesLoaded(): Unit = {
//    var client: DftpClient = null
//    try {
//      // 测试连接 (证明服务器已启动)
//      client = DftpClient.connect(baseUrl)
//      assertNotNull(client, "客户端连接不应为空")
//
//      // 测试 AuthModule (DftpServerStart 的版本接受所有凭证)
//      val credsClient = DftpClient.connect(baseUrl, UsernamePassword("test", "pass"))
//      assertNotNull(credsClient, "使用模拟凭证连接应成功")
//
//      // 测试 FileDirectoryDataSourceModule (是否按预期加载并提供文件)
//      // DftpServerStart 加载了 FileDirectoryDataSourceModule，它应能提供 data/ 目录中的文件
//      val df = client.get(s"$baseUrl/$testFileName")
//      assertNotNull(df, "DataFrame 不应为空")
//
//      // FileDirectoryDataSourceModule 将文件内容作为单列 "text" 返回
//      val expectedSchema = StructType.empty.add("id", LongType).add("value", DoubleType)
//      assertEquals(expectedSchema, df.schema, "FileDirectoryDataSourceModule 的 Schema 不匹配")
//
//      val rows = df.collect()
//      assertEquals(10000, rows.length, "行数不匹配")
//
//      val content = rows.head
//      assertEquals(testFileContent.toString, content.toString, "从服务器检索到的文件内容不匹配")
//    }
//  }
//}