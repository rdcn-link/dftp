/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:05
 * @Modified By:
 */
package link.rdcn.optree

import link.rdcn.operation._
import link.rdcn.optree.fifo.RowFilePipe
import link.rdcn.struct.ValueType.{IntType, StringType}
import link.rdcn.struct._
import link.rdcn.user.Credentials
import org.json.JSONObject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class TransformTreeTest {

  // --- TransformTree.fromJsonString 测试 ---

  @Test
  def testFromJsonString_SourceOp(): Unit = {
    val json = """{"type": "SourceOp", "dataFrameName": "/my/data"}"""
    val op = TransformTree.fromJsonString(json)

    assertNotNull(op, "解析的 Op 不应为 null")
    assertTrue(op.isInstanceOf[SourceOp], "Op 类型应为 SourceOp")
    assertEquals("/my/data", op.asInstanceOf[SourceOp].dataFrameUrl, "dataFrameName 不匹配")
  }

  @Test
  def testFromJsonString_RemoteSourceProxyOp(): Unit = {
    val json = """{"type": "RemoteSourceProxyOp", "baseUrl": "dftp://host.com", "path": "/data", "token": "abc.123"}"""
    val op = TransformTree.fromJsonString(json)

    assertTrue(op.isInstanceOf[RemoteSourceProxyOp], "Op 类型应为 RemoteSourceProxyOp")
    val remoteOp = op.asInstanceOf[RemoteSourceProxyOp]
    assertEquals("dftp://host.com/data", remoteOp.url, "URL 不匹配")
    assertEquals("dftp://host.com:3101", remoteOp.baseUrl, "baseUrl 不匹配")
    assertEquals("/data", remoteOp.path, "path 不匹配")
    assertEquals("abc.123", remoteOp.certificate, "certificate (token) 不匹配")
  }

  @Test
  def testFromJsonString_TransformerNode(): Unit = {
    // 使用一个已知的 TransformFunctionWrapper (PythonCode) 作为 "function"
    val json = """{
      "type": "TransformerNode",
      "function": {"type": "PYTHON_CODE", "code": "print('hi')"},
      "input": []
    }"""
    val op = TransformTree.fromJsonString(json)

    assertTrue(op.isInstanceOf[TransformerNode], "Op 类型应为 TransformerNode")
    val node = op.asInstanceOf[TransformerNode]
    assertNotNull(node.transformFunctionWrapper, "transformFunctionWrapper 不应为 null")
    assertTrue(node.transformFunctionWrapper.isInstanceOf[PythonCode], "Function 类型应为 PythonCode")
    assertEquals("print('hi')", node.transformFunctionWrapper.asInstanceOf[PythonCode].code, "PythonCode 的 code 不匹配")
  }

  @Test
  def testFromJsonString_NestedTree(): Unit = {
    val json = """{
      "type": "Limit",
      "args": [10],
      "input": [
        {
          "type": "SourceOp",
          "dataFrameName": "/my/data"
        }
      ]
    }"""
    val op = TransformTree.fromJsonString(json)

    assertTrue(op.isInstanceOf[LimitOp], "Op 类型应为 LimitOp")
    assertEquals(10, op.asInstanceOf[LimitOp].n, "Limit 的 n 参数不匹配")

    assertEquals(1, op.inputs.length, "应有 1 个 input")
    val inputOp = op.inputs.head
    assertTrue(inputOp.isInstanceOf[SourceOp], "Input Op 类型应为 SourceOp")
    assertEquals("/my/data", inputOp.asInstanceOf[SourceOp].dataFrameUrl, "Input Op 的 dataFrameName 不匹配")
  }

  @Test
  def testFromJsonString_FileRepositoryBundle_In_TransformerNode(): Unit = {
    val json = """{
      "type": "TransformerNode",
      "function": {
        "type": "FileRepositoryBundle",
        "command": ["python", "run.py"],
        "inputFilePath": ["/app/in"],
        "outputFilePath": ["/app/out"],
        "dockerContainer": {
          "containerName": "my-container",
          "imageName": "my-image"
        }
      },
      "input": []
    }"""
    val op = TransformTree.fromJsonString(json)

    assertTrue(op.isInstanceOf[TransformerNode], "Op 类型应为 TransformerNode")
    val wrapper = op.asInstanceOf[TransformerNode].transformFunctionWrapper
    assertTrue(wrapper.isInstanceOf[FileRepositoryBundle], "Function 类型应为 FileRepositoryBundle")

    val bundle = wrapper.asInstanceOf[FileRepositoryBundle]
    assertEquals(Seq("python", "run.py"), bundle.command, "Command 不匹配")
    assertEquals(Seq("/app/in"), bundle.inputFilePath, "inputFilePath 不匹配")
    assertEquals(Seq("/app/out"), bundle.outputFilePath, "outputFilePath 不匹配")
    assertEquals("my-container", bundle.dockerContainer.containerName, "containerName 不匹配")
    assertEquals(Some("my-image"), bundle.dockerContainer.imageName, "imageName 不匹配")
  }


  // --- RemoteSourceProxyOp 测试 ---

  @Test
  def testRemoteSourceProxyOp_Execute_Success(): Unit = {
    val mockCtx = new MockFlowExecutionContextForTransformTree()
    val mockDf = DefaultDataFrame(StructType.empty, Iterator.empty)
    val url = "dftp://host:3101/data"
    val token = "abc.123"

    // 准备模拟的 Context
    mockCtx.remoteDataFrames = Map("dftp://host:3101/data" -> mockDf)

    val op = RemoteSourceProxyOp(url, token)
    val result = op.execute(mockCtx)

    assertEquals(mockDf, result, "返回的 DataFrame 应为模拟的 DataFrame")
  }

  @Test
  def testRemoteSourceProxyOp_Execute_Failure(): Unit = {
    val mockCtx = new MockFlowExecutionContextForTransformTree()
    val url = "dftp://host:3101/data"
    val token = "abc.123"

    // 准备模拟的 Context (返回 None)
    mockCtx.remoteDataFrames = Map.empty

    val op = RemoteSourceProxyOp(url, token)

    val ex = assertThrows(classOf[Exception], () => {
      op.execute(mockCtx)
      ()
    }, "当 loadRemoteDataFrame 返回 None 时应抛出异常")

    assertTrue(ex.getMessage.contains("get remote DataFrame dftp://host:3101/data fail"), "异常消息不匹配")
  }

  @Test
  def testRemoteSourceProxyOp_InvalidUrl(): Unit = {
    // UrlValidator 需要 dftp://, dftps://, http://, https://
    val invalidUrl = "my-invalid-url/data"
    val token = "abc.123"

    assertThrows(classOf[IllegalArgumentException], () => {
      RemoteSourceProxyOp(invalidUrl, token)
      ()
    }, "使用无效 URL 构造 RemoteSourceProxyOp 应抛出 IllegalArgumentException")
  }

  // --- TransformerNode 测试 ---

  @Test
  def testTransformerNode_Contain(): Unit = {
    val child = new MockReleasableTransformerNode("child")
    val parent = TransformerNode(new MockTransformFunctionWrapper("parent", null), child)
    val root = TransformerNode(new MockTransformFunctionWrapper("root", null), parent)
    val orphan = new MockReleasableTransformerNode("orphan")

    assertTrue(root.contain(child), "root 应包含 child")
    assertTrue(root.contain(root), "root 应包含它自己")
    assertFalse(root.contain(orphan), "root 不应包含 orphan")
  }

  @Test
  def testTransformerNode_Release(): Unit = {
    // 准备
    val child = new MockReleasableTransformerNode("child")
    val parent = TransformerNode(new MockTransformFunctionWrapper("parent", null), child)
    val root = TransformerNode(new MockTransformFunctionWrapper("root", null), parent)

    // 执行
    root.release()

    // 验证
    // 验证 release() 被递归调用
    assertTrue(child.released.get(), "子节点的 release() 方法应被调用")
  }

  @Test
  def testTransformerNode_Execute_Sync(): Unit = {
    // 准备
    val mockCtx = new MockFlowExecutionContextForTransformTree(asyncEnabled = false)
    val inputDf = DefaultDataFrame(StructType.empty.add("in", IntType), Seq(Row(1)).iterator)
    val inputOp = MockTransformOpForTransformTree("input", inputDf)

    val expectedDf = DefaultDataFrame(StructType.empty.add("out", IntType), Seq(Row(2)).iterator)
    val mockFunc = new MockTransformFunctionWrapper("func", expectedDf)

    val node = TransformerNode(mockFunc, inputOp)

    // 执行
    val result = node.execute(mockCtx)

    // 验证
    assertEquals(expectedDf, result, "返回的 DataFrame 应为 mockFunc 的结果")
    assertTrue(inputOp.executeCalled, "输入操作的 execute() 应被调用")
    // 验证 mockFunc 是否收到了来自 inputOp 的*真实* DataFrame
    assertEquals(Seq(inputDf), mockFunc.applyCalledWith, "Function 未收到正确的输入 DataFrame")
  }

  @Test
  def testTransformerNode_Execute_Async(): Unit = {
    // 准备
    val mockCtx = new MockFlowExecutionContextForTransformTree(asyncEnabled = true)
    val inputOp = MockTransformOpForTransformTree("input", DefaultDataFrame(StructType.empty, Iterator.empty))
    val mockFunc = new MockTransformFunctionWrapper("func", DefaultDataFrame(StructType.empty, Iterator.empty))

    val node = TransformerNode(mockFunc, inputOp)

    // 执行
    val result = node.execute(mockCtx)

    // 验证
    assertEquals(DataFrame.empty(), result, "异步执行应立即返回 DataFrame.empty()")
    assertTrue(inputOp.executeCalled, "输入操作的 execute() (在异步中) 仍应被调用")

    // 验证异步逻辑
    assertEquals(1, mockCtx.registeredFutures.length, "registerAsyncResult 应被调用 1 次")
    assertEquals(node, mockCtx.registeredFutures.head, "注册的 Op 应为 node 自身")

    // 等待 Future 完成
    Thread.sleep(100)

    // 验证 Future 内部是否使用了 DataFrame.empty() 调用
    assertNotNull(mockFunc.applyCalledWith, "applyToDataFrames 应已被 Future 调用")
    assertEquals(1, mockFunc.applyCalledWith.length, "applyToDataFrames 应收到 1 个 DataFrame")
    assertTrue(mockFunc.applyCalledWith.head.schema.isEmpty, "applyToDataFrames (异步) 应收到 DataFrame.empty()")
  }

  // --- FiFoFileNode 测试 ---

  @Test
  def testFiFoFileNode_Execute(@TempDir tempDir: File): Unit = {
    // 准备
    val pipeFile = new File(tempDir, "test.pipe")
    val filePath = pipeFile.getAbsolutePath
    val expectedData = Seq("hello", "pipe")
    val expectedDfSchema = StructType.empty.add("content", StringType)

    // 创建一个模拟的 inputOp，其*副作用*是写入到 pipeFile
    val inputOp = new TransformOp {
      override def execute(ctx: ExecutionContext): DataFrame = {
        val pipe = new RowFilePipe(pipeFile) // 注意：这里使用了*真实*的 RowFilePipe
        // 在 Windows 上，我们不能 create() (mkfifo)，所以我们直接 write()
        pipe.write(expectedData.iterator)
        DefaultDataFrame(StructType.empty, Iterator.empty) // execute 的返回值被 FiFoFileNode 忽略
      }
      override def operationType: String = "MockWriterOp"
      override def toJson: JSONObject = new JSONObject()

      override var inputs: Seq[TransformOp] = Seq.empty
    }

    val node = FiFoFileNode(filePath, inputOp)
    val mockCtx = new MockFlowExecutionContextForTransformTree()

    // 执行
    val resultDf = node.execute(mockCtx)

    // 验证
    val resultData = resultDf.collect().map(_.getAs[String](0))

    assertEquals(expectedDfSchema, resultDf.schema, "FiFoFileNode 返回的 Schema 不匹配")
    assertEquals(expectedData, resultData, "FiFoFileNode 未能从管道文件中正确读取数据")
  }
}

/**
 * 用于测试 TransformerNode.release() 的模拟类
 */
class MockReleasableTransformerNode(name: String) extends TransformerNode(null) {
  val released = new AtomicBoolean(false)
  override def release(): Unit = released.set(true)
  override def toJson: JSONObject = new JSONObject().put("type", "MockTransformerNode").put("name", name)
}

/**
 * 模拟的 TransformFunctionWrapper
 */
class MockTransformFunctionWrapper(name: String, dfToReturn: DataFrame) extends TransformFunctionWrapper {
  var applyCalledWith: Seq[DataFrame] = null
  var applyCalledContext: FlowExecutionContext = null

  override def toJson: JSONObject = new JSONObject().put("type", "MockFunction").put("name", name)

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    this.applyCalledWith = inputs
    this.applyCalledContext = ctx
    dfToReturn
  }
}

/**
 * 模拟的 TransformOp
 */
case class MockTransformOpForTransformTree(name: String, dfToReturn: DataFrame) extends TransformOp {
  var executeCalled = false
  var executeContext: ExecutionContext = null

  override def execute(ctx: ExecutionContext): DataFrame = {
    this.executeCalled = true
    this.executeContext = ctx
    dfToReturn
  }
  override def operationType: String = "MockOp"
  override def toJson: JSONObject = new JSONObject().put("type", "MockOp").put("name", name)

  override var inputs: Seq[TransformOp] = Seq.empty
}
/**
 * 模拟的 FlowExecutionContext
 */
class MockFlowExecutionContextForTransformTree(asyncEnabled: Boolean = false) extends FlowExecutionContext {
  var registeredFutures = new ArrayBuffer[TransformOp]()
  var remoteDataFrames = Map[String, DataFrame]()

  override def fairdHome: String = "/mock/faird/home"
  override def pythonHome: String = "/mock/python/home"
  override def isAsyncEnabled: Boolean = asyncEnabled

  override def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame], thread: Thread): Unit = {
    registeredFutures.append(transformOp)
  }

  override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = {
    remoteDataFrames.get(baseUrl + path)
  }

  override def getRepositoryClient(): Option[OperatorRepository] = None
  override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = None
}