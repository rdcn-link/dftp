/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:48
 * @Modified By:
 */
package link.rdcn.operation

import jep.SharedInterpreter
import link.rdcn.CommonTestBase.ConfigLoader.dftpConfig
import link.rdcn.CommonTestBase._
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row}
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Base64

class FunctionWrapperJunitTest {
  ConfigLoader.init()
  SharedInterpreterManager.getInterpreter

  // 基础数据
  private val baseRow = Row.fromSeq(Seq(1, 1))
  private val baseRow2 = Row.fromSeq(Seq(2, 2))
  private val dataFrame = DataFrame.fromSeq(Seq(1, 1))
  private val dataFrame1 = DataFrame.fromSeq(Seq(1, 1))
  private val dataFrame2 = DataFrame.fromSeq(Seq(1, 1))
  private val expectedDataFrame = DataFrame.fromSeq(Seq(2, 2))
  private val baseIter = expectedDataFrame.asInstanceOf[DefaultDataFrame].stream
  val ctx = new ExecutionContext {
    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = ???
  }


  @Test
  def testPythonCodeToJson(): Unit = {
    val code = "print('hello')"
    val pythonCode = FunctionWrapper.PythonCode(code)

    val json = pythonCode.toJson

    assertEquals(LangType.PYTHON_CODE.name, json.getString("type"), "JSON type must be PYTHON_CODE")
    assertEquals(code, json.getString("code"), "JSON code must match input code")
  }

  @Test
  def testPythonCodeApplyToInputSingleRow(): Unit = {
    val pythonCode = FunctionWrapper.PythonCode("output_data = 99")
    val result = pythonCode.applyToInput(baseRow, ctx)

    assertEquals(99L, result, "Result from interpreter should be returned")
  }

  @Test
  def testPythonCodeApplyToInputTwoRows(): Unit = {
    val pythonCode = FunctionWrapper.PythonCode("output_data = input_data[0][0] + input_data[1][0]")
    val result = pythonCode.applyToInput((baseRow, baseRow2), ctx)

    assertEquals(3L, result, "Result from interpreter should be returned")
  }

  @Test
  def testPythonCodeApplyToInputIterator(): Unit = {
    val pythonCode = FunctionWrapper.PythonCode("output_data = [[33]]", batchSize = 1)
    val resultIter = pythonCode.applyToInput(baseIter, ctx).asInstanceOf[Iterator[Row]]

    // 触发 hasNext 第一次调用
    val firstRow = resultIter.next()

    assertEquals(33L, firstRow.values.head, "First row should be mapped from mock output")

    // 触发 hasNext 第二次调用 (batchSize=1，所以会再次调用 grouped.hasNext)
    val secondRow = resultIter.next()

    assertEquals(33L, secondRow.values.head, "Second row should be mapped from mock output")

    assertTrue(!resultIter.hasNext, "Iterator should be exhausted")
  }

  @Test()
  def testPythonCodeNoInterpreter(): Unit = {
    val ctx = new ExecutionContext {
      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = ???

      override def getSharedInterpreter(): Option[SharedInterpreter] = None
    } // 无解释器
    val pythonCode = FunctionWrapper.PythonCode("code")
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => pythonCode.applyToInput(baseRow, ctx)
    )
  }

  @Test()
  def testPythonCodeUnsupportedInput(): Unit = {
    val pythonCode = FunctionWrapper.PythonCode("code")

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => pythonCode.applyToInput("unsupported_type", ctx)
    )
    assertEquals("Unsupported input: unsupported_type", exception.getMessage, "Exception message should indicate unsupported input type")

  }

  val mockRowPairFunction = new SerializableFunction[(Row, Row), Any] {
    override def apply(v: (Row, Row)): Any = {
      v._1._1.asInstanceOf[Int] + v._2._1.asInstanceOf[Int]
    }
  }

  val mockIteratorRowFunction = new SerializableFunction[Iterator[Row], Any] {
    override def apply(v: Iterator[Row]): Any = {
      v.map(row=>Row(row._1.asInstanceOf[Int]+1))
    }
  }

  val mockDataFramePairFunction = new SerializableFunction[(DataFrame,DataFrame), DataFrame] {
    override def apply(v: (DataFrame, DataFrame)): DataFrame = {
      val left: List[Int] = v._1.collect().map(row => row._1.asInstanceOf[Int])
      val right: List[Int] = v._2.collect().map(row => row._1.asInstanceOf[Int])
      DataFrame.fromSeq(left.zip(right).map { case (a, b) => a + b })
    }
  }

  // 创建一个 Base64 编码的序列化 MockGenericFunctionCall
  private val serializedSingleRowCall: String = {
    val bytes = FunctionSerializer.serialize(SingleRowCall(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = v1
    }))
    Base64.getEncoder.encodeToString(bytes)
  }

  private val serializedRowPairCall: String = {
    val bytes = FunctionSerializer.serialize(new RowPairCall(mockRowPairFunction))
    Base64.getEncoder.encodeToString(bytes)
  }

  private val serializedIteratorRowCall: String = {
    val bytes = FunctionSerializer.serialize(new IteratorRowCall(mockIteratorRowFunction))
    Base64.getEncoder.encodeToString(bytes)
  }

  private val serializedDataFrameCall11: String = {
    val bytes = FunctionSerializer.serialize(new DataFrameCall11(new SerializableFunction[DataFrame, DataFrame] {
      override def apply(v1: DataFrame): DataFrame = v1
    }))
    Base64.getEncoder.encodeToString(bytes)
  }

  private val serializedDataFrameCall21: String = {
    val bytes = FunctionSerializer.serialize(new DataFrameCall21(mockDataFramePairFunction))
    Base64.getEncoder.encodeToString(bytes)
  }

  @Test
  def testJavaBintoJson(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedSingleRowCall)
    val json = javaBin.toJson

    assertEquals(LangType.JAVA_BIN.name, json.getString("type"), "JSON type must be JAVA_BIN")
    assertEquals(serializedSingleRowCall, json.getString("serializedBase64"), "JSON serializedBase64 must match input")
  }

  @Test
  def testJavaBinLazyValDeserialization(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedSingleRowCall)

    assertTrue(javaBin.genericFunctionCall.isInstanceOf[SingleRowCall], "GenericFunctionCall should be deserialized")
  }

  @Test
  def testJavaBinApplyToInputSingleRow(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedSingleRowCall)
    val result = javaBin.applyToInput(baseRow, ctx)

    assertTrue(result.isInstanceOf[Row], "Result should be a Row from transform")
    assertEquals(1, result.asInstanceOf[Row].values.head, "Row value should be transformed (1*2)")
  }

  @Test
  def testJavaBinApplyToInputTwoRows(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedRowPairCall)
    val result = javaBin.applyToInput((baseRow, baseRow2), ctx)

    assertEquals(3, result.asInstanceOf[Int], "Mock transform should receive the Tuple")
  }

  @Test
  def testJavaBinApplyToInputIterator(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedIteratorRowCall)
    val result = javaBin.applyToInput(baseIter, ctx)

    assertTrue(result.isInstanceOf[Iterator[_]], "Mock transform should receive the Iterator")
    assertEquals(3, result.asInstanceOf[Iterator[Row]].next()._1, "Mock transform should receive the Iterator")
  }

  @Test
  def testJavaBinApplyToInputdataFrame(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedDataFrameCall11)
    val result = javaBin.applyToInput(dataFrame, ctx)

    assertEquals(dataFrame, result, "Mock transform should receive the DataFrame")
  }

  // 覆盖 case dfs: (DataFrame, DataFrame) => ...
  @Test
  def testJavaBinApplyToInputTwoDataFrames(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedDataFrameCall21)
    val result = javaBin.applyToInput((dataFrame1, dataFrame2), ctx)

    assertEquals(2, result.asInstanceOf[DefaultDataFrame].collect().head._1, "Mock transform should receive the Tuple of DataFrames")
  }


  @Test
  def testJavaBinUnsupportedInput(): Unit = {
    val javaBin = FunctionWrapper.JavaBin(serializedSingleRowCall)

    val exception = assertThrows(
      classOf[IllegalArgumentException], () => javaBin.applyToInput("unsupported_type", ctx))
    assertEquals(s"Unsupported input: unsupported_type", exception.getMessage)

  }

  @Test
  def testFunctionWrapperApplyPythonCode(): Unit = {
    val json = new JSONObject()
    json.put("type", LangType.PYTHON_CODE.name)
    json.put("code", "pass")

    val wrapper = FunctionWrapper(json)

    assertTrue(wrapper.isInstanceOf[FunctionWrapper.PythonCode], "Factory should return PythonCode")
  }

  @Test
  def testFunctionWrapperApply_JavaBin(): Unit = {
    val json = new JSONObject()
    json.put("type", LangType.JAVA_BIN.name)
    json.put("serializedBase64", serializedSingleRowCall)

    val wrapper = FunctionWrapper(json)

    assertTrue(wrapper.isInstanceOf[FunctionWrapper.JavaBin], "Factory should return JavaBin")
  }


  @Test
  def testGetJavaSerialized(): Unit = {

    // 覆盖 getJavaSerialized
    val javaBin = FunctionWrapper.getJavaSerialized(SingleRowCall(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = v1
    }))

    assertTrue(javaBin.isInstanceOf[FunctionWrapper.JavaBin], "Result should be JavaBin")
    assertTrue(javaBin.serializedBase64.length > 0, "Base64 string should not be empty")

    // 验证反向解码的类型是否正确 (通过 lazy val 触发)
    assertTrue(javaBin.genericFunctionCall.isInstanceOf[SingleRowCall], "Deserialized content should be MockGenericFunctionCall")
  }
}