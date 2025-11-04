package link.rdcn

import jep.SharedInterpreter
import link.rdcn.operation.SharedInterpreterManager
import link.rdcn.optree._
import link.rdcn.struct._
import link.rdcn.user.Credentials
import org.junit.jupiter.api.Test

import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 15:42
 * @Modified By:
 */
class FunctionWrapperTest {
  val rows = Seq(Row.fromSeq(Seq(1, 2))).iterator
  val dataFrame = DefaultDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
  val dataFrames = Seq(dataFrame)
  val fairdHome = Paths.get(getClass.getClassLoader.getResource("").toURI()).toString

  @Test
  def pythonBinTest(): Unit = {
    val whlPath = Paths.get(fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val pythonBin = PythonBin("normalize",whlPath)
    val df = pythonBin.applyToDataFrames(dataFrames, ctx)
    df.foreach(row => {
      assert(row._1 == 0.33)
      assert(row._2 == 0.67)
    })
  }

  @Test
  def javaJarTest(): Unit = {
    val javaJar = JavaJar(Paths.get(fairdHome, "faird-plugin-impl-0.5.0-20250920.jar").toString(),"Transformer11")
    val newDataFrame = javaJar.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assert(row._1 == 1)
      assert(row._2 == 2)
      assert(row._3 == 100)
    })
  }

  @Test
  def cppBinTest(): Unit = {
    val cppPath = Paths.get(fairdHome, "lib", "cpp", "cpp_processor.exe").toString
    val cppBin = CppBin(cppPath)
    val newDf = cppBin.applyToInput(dataFrames, ctx).asInstanceOf[DataFrame]
    newDf.foreach(row => {
      assert(row._1 == true)
    })
  }

  private def runCppProcess(cppPath: String, inputPath: String, outputPath: String): Int = {
    val pb = new ProcessBuilder(cppPath, inputPath, outputPath)
    pb.inheritIO() // 继承当前进程的 stdout/stderr，调试时很有用

    val process = pb.start()
    val exitCode = process.waitFor()
    exitCode
  }

  def ctx: FlowExecutionContext = new FlowExecutionContext {

    override val pythonHome: String = ""

    override def getSharedInterpreter(): Option[SharedInterpreter] = Some(SharedInterpreterManager.getInterpreter)

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = ???

    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))

    override val fairdHome: String = this.fairdHome

    override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = ???
  }

}

