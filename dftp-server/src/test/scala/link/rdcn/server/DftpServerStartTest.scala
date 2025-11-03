package link.rdcn.server

import link.rdcn.client.DftpClient
import link.rdcn.server.DftpServerStartTest.{baseUrl, testFileContent, testFileName}
import link.rdcn.server.module.{AuthModule, BaseDftpModule, DirectoryDataSourceModule}
import link.rdcn.struct.StructType
import link.rdcn.struct.ValueType.StringType
import link.rdcn.user._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.nio.file.{Path, Paths}
import java.util.Properties

object DftpServerStartTest {
  private var server: DftpServer = _
  private val serverPort = 3103 // 为本测试使用一个唯一的端口
  private val baseUrl = s"dftp://0.0.0.0:$serverPort"
  private val testFileName = "start_test.txt"
  private val testFileContent = "Hello DftpServerStart"

  @TempDir
  private var homeDir: File = _
  private var dataDir: File = _

  @BeforeAll
  def startServer(): Unit = {
    // --- DftpServerStart 所需的模拟 dftpHome 环境 ---
    homeDir = new File("")
    val dftpHome = homeDir.getAbsolutePath
    val confDir = new File(dftpHome, "conf")
    dataDir = new File(dftpHome, "data")
    confDir.mkdirs()
    dataDir.mkdirs()

    // --- 创建模拟 dftp.conf 文件 ---
    val confFile = new File(confDir, "dftp.conf")
    val writer = new PrintWriter(new FileOutputStream(confFile))
    try {
      writer.println(s"dftp.host.position=0.0.0.0")
      writer.println(s"dftp.host.port=$serverPort")
    } finally {
      writer.close()
    }

    // --- 创建模拟数据文件 (用于测试 DirectoryDataSourceModule) ---
    val dataFile = new File(dataDir, testFileName)
    val fileWriter = new PrintWriter(new FileOutputStream(dataFile))
    try {
      fileWriter.println(testFileContent)
    } finally {
      fileWriter.close()
    }

    // --- 模拟 DftpServerStart.main 中的加载和组装逻辑 ---

    // 模拟 loadProperties
    val props = new Properties()
    val reader = new FileInputStream(confFile)
    try {
      props.load(reader)
    } finally {
      reader.close()
    }

    // 模拟创建 DftpServerConfig
    val dftpServerConfig = DftpServerConfig(
      props.getProperty("dftp.host.position"),
      props.getProperty("dftp.host.port").toInt,
      Some(dftpHome)
    )

    // 模拟 DftpServerStart.authenticationService
    val authenticationService = new AuthenticationService {
      override def accepts(request: AuthenticationRequest): Boolean = true
      override def authenticate(credentials: Credentials): UserPrincipal =
        UserPrincipalWithCredentials(credentials)
    }

    val directorySourceModule = new DirectoryDataSourceModule
    directorySourceModule.setRootDirectory(dataDir)

    // 模拟服务器组装
    server = new DftpServer(dftpServerConfig)
    server.addModule(new BaseDftpModule)
      .addModule(new AuthModule(authenticationService)) // 假设 AuthModule 构造函数如 DftpServerStart 所示
      .addModule(directorySourceModule)

    // 启动服务器 (使用非阻塞的 start() 代替 startBlocking())
    server.start()
    // 等待服务器启动
    Thread.sleep(2000)
  }

  @AfterAll
  def stopServer(): Unit = {
    if (server != null) {
      server.close()
    }
  }
}

/**
 * 针对 DftpServerStart 的测试用例。
 */
class DftpServerStartTest {

  /**
   * 测试 main 方法的参数校验逻辑。
   */
  @Test
  def testMainArgsParsingError(): Unit = {
    val ex = assertThrows(classOf[RuntimeException], () => {
      // 调用 main，期望它因缺少参数而立即失败
      DftpServerStart.main(Array.empty)
      ()
    }, "DftpServerStart.main 应该在没有参数时抛出异常")

    // 检查是否符合用户习惯
    assertTrue(ex.getMessage.toLowerCase.contains("need set dftp home"), "异常消息应指明需要 dftp home")
  }

  /**
   * 通过 API 测试服务器是否已按 DftpServerStart 的配置正确启动。
   */
  @Test
  def testServerStartedAndModulesLoaded(): Unit = {
    var client: DftpClient = null
    try {
      // 1. 测试连接 (证明服务器已启动)
      client = DftpClient.connect(baseUrl)
      assertNotNull(client, "客户端连接不应为空")

      // 2. 测试 AuthModule (DftpServerStart 的版本接受所有凭证)
      val credsClient = DftpClient.connect(baseUrl, UsernamePassword("test", "pass"))
      assertNotNull(credsClient, "使用模拟凭证连接应成功")
      credsClient.close()

      // 3. 测试 DirectoryDataSourceModule (是否按预期加载并提供文件)
      // DftpServerStart 加载了 DirectoryDataSourceModule，它应能提供 data/ 目录中的文件
      val df = client.get(s"$baseUrl/$testFileName")
      assertNotNull(df, "DataFrame 不应为空")

      // DirectoryDataSourceModule 将文件内容作为单列 "text" 返回
      val expectedSchema = StructType.empty.add("text", StringType)
      assertEquals(expectedSchema, df.schema, "DirectoryDataSourceModule 的 Schema 不匹配")

      val rows = df.collect()
      assertEquals(1, rows.length, "应只检索到一行")

      val content = rows.head
      assertEquals(testFileContent, content, "从服务器检索到的文件内容不匹配")

    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName))) // 先到test-classes中查找，然后到classes中查找
      .getOrElse(throw new RuntimeException())
    val nativePath: Path = Paths.get(url.toURI())
    nativePath.toString
  }
}