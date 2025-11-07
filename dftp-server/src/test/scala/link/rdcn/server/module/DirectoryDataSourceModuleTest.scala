/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 18:28
 * @Modified By:
 */
package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.struct.ValueType.{BlobType, LongType, RefType, StringType}
import link.rdcn.struct._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io._
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

class DirectoryDataSourceModuleTest {

  // JUnit 5 会为每个测试注入一个唯一的临时目录
  @TempDir
  var tempDir: File = _

  private var moduleToTest: DirectoryDataSourceModule = _
  private var mockOldService: MockDataFrameProviderServiceForDirectory = _
  private var hookedEventHandler: EventHandler = _
  private var defaultRootDirectory: File = _
  private val oldDF: DataFrame = DefaultDataFrame(StructType.empty.add("old", StringType), Seq(Row("old")).iterator)

  // 模拟一个 ServerContext
  implicit private val mockContext: ServerContext = new MockServerContextForDirectory()

  @BeforeEach
  def setUp(): Unit = {
    // 1. 设置文件系统
    defaultRootDirectory = new File(tempDir, "data")
    defaultRootDirectory.mkdirs()

    // 2. 创建模拟文件
    // 创建 CSV
    val csvFile = new File(defaultRootDirectory, "test.csv")
    val csvWriter = new PrintWriter(new FileWriter(csvFile))
    csvWriter.println("name,age")
    csvWriter.println("Alice,30")
    csvWriter.println("Bob,25")
    csvWriter.close()

    // 创建 Blob (txt)
    val blobFile = new File(defaultRootDirectory, "blob.txt")
    val blobWriter = new PrintWriter(new FileWriter(blobFile))
    blobWriter.print("This is blob content")
    blobWriter.close()

    // 创建子目录和文件
    val subDir = new File(defaultRootDirectory, "subdir")
    subDir.mkdirs()
    val subFile = new File(subDir, "nested.txt")
    val subWriter = new PrintWriter(new FileWriter(subFile))
    subWriter.print("Nested file")
    subWriter.close()

    // 3. 准备模块和模拟服务
    mockOldService = new MockDataFrameProviderServiceForDirectory("OldService")
    moduleToTest = new DirectoryDataSourceModule()

    // 4. 关键：将被测模块指向我们的临时目录
    moduleToTest.setRootDirectory(defaultRootDirectory)

    // 5. 执行 init
    val mockAnchor = new MockAnchorForDirectory()
    moduleToTest.init(mockAnchor, mockContext)
    hookedEventHandler = mockAnchor.hookedHandler
    assertNotNull(hookedEventHandler, "init() 方法未能向 Anchor 注册 EventHandler")
  }

  /**
   * 提取链式服务以供测试
   */
  private def getChainedService(oldService: DataFrameProviderService = null): DataFrameProviderService = {
    val holder = new ObjectHolder[DataFrameProviderService]()
    if (oldService != null) {
      holder.set(oldService)
    }
    val event = RequireDataFrameProviderEvent(holder)
    hookedEventHandler.doHandleEvent(event)
    holder.invoke(run = s => s, onNull = null)
  }

  // --- 链式逻辑测试 ---

  /**
   * 测试链式逻辑: InnerService (DirectoryDataSource) 接受
   */
  @Test
  def testChainingLogic_InnerServiceAccepts(): Unit = {
    // 准备: OldService 不接受
    mockOldService.acceptsUrl = false
    val chainedService = getChainedService(mockOldService)

    val testUrl = "dftp://mock-host:1234/test.csv" // 这个文件存在于 @TempDir 中

    // 验证 accepts()
    assertTrue(chainedService.accepts(testUrl), "链式 accepts() 应返回 true (因为 InnerService 接受)")

    // 验证 getDataFrame()
    val resultDF = chainedService.getDataFrame(testUrl, MockUserForDirectory)

    assertEquals(2, resultDF.collect().length, "InnerService (Directory) 未能正确加载 CSV")

    // 验证调用
    assertFalse(mockOldService.acceptsCalled, "OldService.accepts 不应被调用（因为 Inner 优先）")
    assertFalse(mockOldService.getDataFrameCalled, "OldService.getDataFrame 不应被调用")
  }

  /**
   * 测试链式逻辑: InnerService 拒绝, OldService 接受
   */
  @Test
  def testChainingLogic_OldServiceAccepts(): Unit = {
    // 准备: OldService 接受, 并返回其模拟 DF
    mockOldService.acceptsUrl = true
    mockOldService.dfToReturn = oldDF
    val chainedService = getChainedService(mockOldService)

    val testUrl = "http://old.com/data" // 这个文件*不*存在于 @TempDir 中

    // 验证 accepts()
    assertTrue(chainedService.accepts(testUrl), "链式 accepts() 应返回 true (因为 OldService 接受)")

    // 验证 getDataFrame()
    val resultDF = chainedService.getDataFrame(testUrl, MockUserForDirectory)

    assertEquals(oldDF, resultDF, "链式 getDataFrame() 应返回 OldService 的 DataFrame")

    // 验证调用
    assertTrue(mockOldService.getDataFrameCalled, "OldService.getDataFrame 应被调用")
    assertEquals(testUrl, mockOldService.urlChecked, "OldService 检查了错误的 URL")
  }

  /**
   * 测试链式逻辑: 两个服务都拒绝
   */
  @Test
  def testChainingLogic_NoServiceAccepts(): Unit = {
    // 准备: OldService 也不接受
    mockOldService.acceptsUrl = false
    val chainedService = getChainedService(mockOldService)

    val testUrl = "http://none.com/data" // 这个文件*不*存在于 @TempDir 中

    // 验证 accepts()
    assertFalse(chainedService.accepts(testUrl), "链式 accepts() 应返回 false (因为两者都拒绝)")

    // 验证 getDataFrame() 抛出异常
    val ex = assertThrows(classOf[DataFrameNotFoundException], () => {
      chainedService.getDataFrame(testUrl, MockUserForDirectory)
      ()
    }, "链式 getDataFrame() 在两者都拒绝时应抛出 DataFrameNotFoundException")

    assertTrue(ex.getMessage.contains(testUrl), "异常消息应包含未找到的 URL")
  }

  // --- 文件系统 I/O (getDataFrameByUrl) 测试 ---

  @Test
  def testGetDataFrame_CSVFile(): Unit = {
    val chainedService = getChainedService() // 无需 old service
    val testUrl = "dftp://mock-host:1234/test.csv"

    val resultDF = chainedService.getDataFrame(testUrl, MockUserForDirectory)

    val expectedSchema = StructType.empty.add("name", StringType).add("age", LongType)
    assertEquals(expectedSchema.toString, resultDF.schema.toString, "CSV 文件的 Schema 不匹配") // 比较 toString 避免类型推断 (e.g., Int vs String)

    val rows = resultDF.collect().map(_.toSeq)
    assertEquals(ArrayBuffer("Alice", "30").toString(), rows(0).toString(), "CSV 第一行数据不匹配")
    assertEquals(ArrayBuffer("Bob", "25").toString(), rows(1).toString(), "CSV 第二行数据不匹配")
  }

  @Test
  def testGetDataFrame_BlobFile(): Unit = {
    val chainedService = getChainedService()
    val testUrl = "dftp://mock-host:1234/blob.txt"

    val resultDF = chainedService.getDataFrame(testUrl, MockUserForDirectory)

    assertEquals(StructType.empty.add("_1",BlobType).toString, resultDF.schema.toString, "Blob 文件的 Schema 不匹配")

    val rows = resultDF.collect()
    assertEquals(1, rows.length, "Blob 文件应只返回 1 行")

    val blob = rows.head.get(0).asInstanceOf[Blob]
    val verifier = new StreamVerifier()
    blob.offerStream(verifier)
    assertEquals("This is blob content", verifier.readContent, "Blob 文件内容不匹配")
  }

  @Test
  def testGetDataFrame_ListRootDirectory(): Unit = {
    val chainedService = getChainedService()
    val testUrl = "dftp://mock-host:1234/" // 根目录

    val resultDF = chainedService.getDataFrame(testUrl, MockUserForDirectory)

    // 验证 Schema (来自 DataUtils.listFilesWithAttributes)
    val expectedSchema = StructType.binaryStructType.add("url", RefType)
    assertEquals(expectedSchema, resultDF.schema, "目录列表的 Schema 不匹配")

    // 验证内容
    val rows = resultDF.collect().map(_.getAs[String](0)).sorted // 按名称排序以确保断言稳定
    val expectedFiles = Seq("blob.txt", "subdir", "test.csv").sorted

    assertEquals(expectedFiles, rows, "根目录列表的内容不匹配")
  }

  @Test
  def testGetDataFrame_ListSubDirectory(): Unit = {
    val chainedService = getChainedService()
    val testUrl = "dftp://mock-host:1234/subdir" // 子目录

    val resultDF = chainedService.getDataFrame(testUrl, MockUserForDirectory)

    // 验证 Schema
    val expectedSchema = StructType.binaryStructType.add("url", RefType)
    assertEquals(expectedSchema, resultDF.schema, "子目录列表的 Schema 不匹配")

    // 验证内容
    val rows = resultDF.collect()
    assertEquals(1, rows.length, "子目录应只包含 1 个文件")

    val row = rows.head
    assertEquals("nested.txt", row.getAs[String](0), "子目录中的文件名不匹配")
    assertEquals("application/octet-stream", row.getAs[String](2), "子目录中的文件类型不匹配")

    // 验证 DFRef
    val expectedRefUrl = "dftp://mock-host:1234/subdir\\nested.txt"
    assertEquals(expectedRefUrl, row._8.asInstanceOf[DFRef].url, "文件的 DFRef URL 不匹配")
  }
}

class StreamVerifier extends (InputStream => String) {
  var readContent: String = ""
  var streamClosed: Boolean = false

  override def apply(stream: InputStream): String = {
    // 检查 stream 是否是 FileInputStream 的实例
    assertTrue(stream.isInstanceOf[FileInputStream], "Stream passed to consumer must be a FileInputStream")

    // 读取内容
    val contentBytes = new Array[Byte](stream.available())
    stream.read(contentBytes)
    readContent = new String(contentBytes, StandardCharsets.UTF_8)

    // 尝试关闭，如果成功则证明流未在外部关闭
    try {
      stream.close()
      streamClosed = true
    } catch {
      case _: IOException => // ignore
    }

    readContent
  }
}