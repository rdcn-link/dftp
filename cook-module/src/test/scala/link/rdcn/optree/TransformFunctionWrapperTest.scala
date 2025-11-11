package link.rdcn.optree

import jep.SharedInterpreter
import link.rdcn.dacp.optree.fifo.DockerContainer
import link.rdcn.dacp.optree.{CppBin, FileRepositoryBundle, FlowExecutionContext, JavaBin, JavaCode, JavaJar, OperatorRepository, PythonBin, PythonCode, RepositoryClient, RepositoryOperator, TransformFunctionWrapper}
import link.rdcn.operation.{ExecutionContext, GenericFunctionCall, SharedInterpreterManager}
import link.rdcn.struct._
import link.rdcn.user.Credentials
// 修复：添加 JUnit 断言
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{Disabled, Test}

import java.io.File
import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 15:42
 */
class TransformFunctionWrapperTest {
  val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
  val dataFrame = DefaultDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
  val dataFrames = Seq(dataFrame)
  // 确保 fairdHome 在 ctx 初始化之前被定义
  val fairdHome = Paths.get(getClass.getClassLoader.getResource("").toURI()).toString


  @Test
  @Disabled("这是一个集成测试，需要 JEP 和一个有效的 Python 环境 ('python.home' Java 属性)")
  def pythonBinTest(): Unit = {
    val whlPath = Paths.get(fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val pythonBin = PythonBin("normalize", whlPath)
    val df = pythonBin.applyToDataFrames(dataFrames, ctx)
    df.foreach(row => {
      // 修复：使用 assertEquals 并为浮点数添加 delta
      assertEquals(0.33, row._1.asInstanceOf[Double], 0.01, "归一化后的 col_1 不匹配")
      assertEquals(0.67, row._2.asInstanceOf[Double], 0.01, "归一化后的 col_2 不匹配")
    })
  }

  @Test
  def javaJarTest(): Unit = {
    val jarName = "dftp-plugin-impl-0.5.0-20250910.jar"
    val jarPath = Paths.get(fairdHome, "lib", "java", jarName).toString

    // 检查 jar 文件是否存在，如果不存在则跳过测试
    if (!new File(jarPath).exists()) {
      println(s"警告：跳过 javaJarTest，因为 $jarName 未在 test/resources/lib/java 中找到")
      return
    }

    val javaJar = JavaJar(jarPath, "Transformer11")
    val newDataFrame = javaJar.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      // 修复：使用 assertEquals
      assertEquals(1, row.getAs[Int](0), "JavaJar col_1 不匹配")
      assertEquals(2, row.getAs[Int](1), "JavaJar col_2 不匹配")
      assertEquals(100, row.getAs[Int](2), "JavaJar col_3 (新增) 不匹配")
    })
  }

  @Test
  def cppBinTest(): Unit = {
    // 修复：根据操作系统选择正确的 C++ 可执行文件
    val osName = System.getProperty("os.name").toLowerCase()
    val cppExecutableName = if (osName.contains("win")) "cpp_processor.exe" else "cpp_processor"
    val cppPath = Paths.get(fairdHome, "lib", "cpp", cppExecutableName).toString

    // 检查 C++ 可执行文件是否存在，如果不存在则跳过测试
    if (!new File(cppPath).exists()) {
      println(s"警告：跳过 cppBinTest，因为 $cppExecutableName 未在 test/resources/lib/cpp 中找到")
      return
    }

    val cppBin = CppBin(cppPath)
    val newDf = cppBin.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDf.foreach(row => {
      // 修复：使用 assertEquals
      assertEquals(true, row.getAs[Boolean](0), "CppBin 返回值不匹配")
    })
  }

  // --- 补充的新测试用例 ---

  /**
   * 测试 PythonCode (JEP 集成)
   */
  @Test
  @Disabled("这是一个集成测试，需要 JEP 和一个有效的 Python 环境 ('python.home' Java 属性)")
  def testPythonCode_Integration(): Unit = {
    // 这个 Python 代码将 input_data (1, 2) 转换为 (2, 4)
    val code = "output_data = [ [r[0] * 2, r[1] * 2] for r in input_data ]"
    val pythonCode = PythonCode(code)

    val newDf = pythonCode.applyToDataFrames(dataFrames, ctx)
    val results = newDf.collect()

    assertEquals(1, results.length, "PythonCode 应返回 1 行")
    val row = results.head
    assertEquals(2, row.getAs[Int](0), "PythonCode 处理后的 col_1 不匹配 (1*2)")
    assertEquals(4, row.getAs[Int](1), "PythonCode 处理后的 col_2 不匹配 (2*2)")
  }

  /**
   * 测试 JavaBin (序列化函数) 的执行
   */
  @Test
  def testJavaBin_Execution(): Unit = {
    // 创建一个可序列化的 GenericFunctionCall
    val func = new GenericFunctionCall with Serializable {
      override def transform(input: Any): Any = {
        val df = input.asInstanceOf[DataFrame]
        df.map(row => Row.fromSeq(Seq(row._1.asInstanceOf[Int] + 1, row._2.asInstanceOf[Int] + 1)))
      }
    }

    // 序列化
    val javaBin = TransformFunctionWrapper.getJavaSerialized(func)

    // 执行
    val newDf = javaBin.applyToDataFrames(dataFrames, ctx)
    val results = newDf.collect()

    // 验证
    assertEquals(1, results.length, "JavaBin 应返回 1 行")
    val row = results.head
    assertEquals(2, row.getAs[Int](0), "JavaBin col_1 不匹配")
    assertEquals(3, row.getAs[Int](1), "JavaBin col_2 不匹配")
  }

  /**
   * 测试 RepositoryOperator (模拟 Context)
   */
  @Test
  def testRepositoryOperator_Execution(): Unit = {
    val expectedDf = DefaultDataFrame(StructType.empty, Iterator.empty) // 模拟的返回结果
    val operatorId = "test-operator-id"

    // 创建一个模拟的 RepositoryClient
    val mockRepoClient = new OperatorRepository {
      override def executeOperator(functionId: String, inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
        // 验证传入的参数
        assertEquals(operatorId, functionId, "传入的 functionID 不匹配")
        assertEquals(dataFrames, inputs, "传入的 inputs (DataFrames) 不匹配")
        expectedDf // 返回模拟的 DataFrame
      }
    }

    // 创建一个返回模拟 Client 的 Mock Context
    val mockCtx = new FlowExecutionContext {
      override def fairdHome: String = ""

      override def pythonHome: String = ""

      override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = None

      override def getRepositoryClient(): Option[OperatorRepository] = Some(mockRepoClient)

      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
    }

    // 执行
    val repoOp = RepositoryOperator(operatorId)
    val resultDf = repoOp.applyToDataFrames(dataFrames, mockCtx)

    // 验证
    assertEquals(expectedDf, resultDf, "RepositoryOperator 返回的 DataFrame 不匹配")
  }

  /**
   * 测试 applyToInput 的类型检查
   */
  @Test
  def testApplyToInput_TypeCheck(): Unit = {
    val wrapper = PythonCode("test") // 使用一个简单的实例

    // 测试非 Seq[DataFrame] 输入
    assertThrows(classOf[IllegalArgumentException], () => {
      wrapper.applyToInput("not a sequence", ctx)
      ()
    }, "输入不是 Seq[DataFrame] 时应抛出 IllegalArgumentException")

    // 测试非 FlowExecutionContext 输入
    val invalidCtx = new ExecutionContext {
      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
    }
    assertThrows(classOf[IllegalArgumentException], () => {
      wrapper.applyToInput(dataFrames, invalidCtx)
      ()
    }, "Context 不是 FlowExecutionContext 时应抛出 IllegalArgumentException")
  }


  // --- JSON 序列化/反序列化（往返）测试 ---

  @Test
  def testJsonRoundTrip_PythonCode(): Unit = {
    val original = PythonCode("print('hello')", 50)
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original.toString(), deserialized.toString(), "PythonCode JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_JavaBin(): Unit = {
    val original = JavaBin("base64string==")
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "JavaBin JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_JavaCode(): Unit = {
    val original = JavaCode("public class Test {}")
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "JavaCode JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_PythonBin(): Unit = {
    val original = PythonBin("my_func", "/path/to/my.whl", 50)
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "PythonBin JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_JavaJar(): Unit = {
    val original = JavaJar("/path/to/my.jar", "Transformer11")
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "JavaJar JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_CppBin(): Unit = {
    val original = CppBin("/path/to/my.exe")
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "CppBin JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_RepositoryOperator(): Unit = {
    val original = RepositoryOperator("operator-id-123")
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "RepositoryOperator JSON 往返失败")
  }

  @Test
  def testJsonRoundTrip_FileRepositoryBundle(): Unit = {
    val container = DockerContainer("test-container", Some("/host"), Some("/cont"), Some("img"))
    val original = FileRepositoryBundle(
      command = Seq("python", "run.py"),
      inputFilePath = Seq("/cont/in.fifo"),
      outputFilePath = Seq("/cont/out.fifo"),
      dockerContainer = container
    )
    val json = original.toJson
    val deserialized = TransformFunctionWrapper.fromJsonObject(json)

    assertEquals(original, deserialized, "FileRepositoryBundle JSON 往返失败")
    assertEquals(original.dockerContainer, deserialized.asInstanceOf[FileRepositoryBundle].dockerContainer,
      "FileRepositoryBundle 嵌套的 DockerContainer JSON 往返失败")
  }


  // --- 修复后的 ctx 方法 ---

  private def runCppProcess(cppPath: String, inputPath: String, outputPath: String): Int = {
    val pb = new ProcessBuilder(cppPath, inputPath, outputPath)
    pb.inheritIO() // 继承当前进程的 stdout/stderr，调试时很有用

    val process = pb.start()
    val exitCode = process.waitFor()
    exitCode
  }

  def ctx: FlowExecutionContext = new FlowExecutionContext {

    // 假设 JEP/Python 环境已在某处配置
    override val pythonHome: String = System.getProperty("python.home", "")

    override def getSharedInterpreter(): Option[SharedInterpreter] = {
      if (pythonHome.isEmpty) {
        println("警告：跳过 getSharedInterpreter，因为 'python.home' 未设置")
        None
      } else {
        Some(SharedInterpreterManager.getInterpreter)
      }
    }

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None // 模拟

    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))

    // 修复：此处的 'fairdHome' 应引用外部类的 'fairdHome'
    override val fairdHome: String = TransformFunctionWrapperTest.this.fairdHome

    override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = None // 模拟
  }

}