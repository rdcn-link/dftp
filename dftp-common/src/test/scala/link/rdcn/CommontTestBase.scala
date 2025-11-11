package link.rdcn

import link.rdcn.struct._

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicInteger


/** *
 * 所有测试用相关公共类和变量
 */
trait CommonTestBase {

}

object CommonTestBase {

  // 文件数量配置
  val binFileCount = 3
  val csvFileCount = 3
  val excelFileCount = 3

  val resourceUrl = getClass.getProtectionDomain.getCodeSource.getLocation
  val testClassesDir = new File(resourceUrl.toURI)


  def getOutputDir(subDirs: String*): String = {
    val outDir = Paths.get(testClassesDir.getParentFile.getParentFile.getAbsolutePath, subDirs: _*) // 项目根路径
    Files.createDirectories(outDir)
    outDir.toString
  }

  /**
   *
   * @param resourceName
   * @return test下名为resourceName的文件夹
   */
  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName))) // 先到test-classes中查找，然后到classes中查找
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    val nativePath: Path = Paths.get(url.toURI())
    nativePath.toString
  }

  def listFiles(directoryPath: String): Seq[File] = {
    val dir = new File(directoryPath)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles().filter(_.isFile).toSeq
    } else {
      Seq.empty
    }
  }

  case class DataFrameInfo(
                            name: String,
                            path: URI,
                            inputSource: InputSource,
                            schema: StructType
                          ) {
  }

  case class DataSet(
                      dataSetName: String,
                      dataSetId: String,
                      dataFrames: List[DataFrameInfo]
                    ) {
    /** 生成 RDF 元数据模型 */
    def getMetadata(): Unit = {}

    def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
      dataFrames.find { dfInfo =>
        val normalizedDfPath: String = dfInfo.path.toString
        normalizedDfPath.contains(dataFrameName)
      }
    }
  }

  sealed trait InputSource

  case class CSVSource(
                        delimiter: String = ",",
                        head: Boolean = false
                      ) extends InputSource

  case class JSONSource(
                         multiline: Boolean = false
                       ) extends InputSource

  case class DirectorySource(
                              recursive: Boolean = true
                            ) extends InputSource

  case class StructuredSource() extends InputSource

  case class ExcelSource() extends InputSource



  abstract class DataProviderImpl {
    val dataSetsScalaList: List[DataSet]
    val dataFramePaths: (String => String)

    def getDataStreamSource(dataFrameName: String): DataStreamSource = {
      val dataFrameInfo: DataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return new DataStreamSource {
        override def rowCount: Long = -1

        override def schema: StructType = StructType.empty

        override def iterator: ClosableIterator[Row] = ClosableIterator(Iterator.empty)()
      })
      dataFrameInfo.inputSource match {
        case _: CSVSource => DataStreamSource.csv(new File(dataFrameInfo.path))
        case _: DirectorySource => DataStreamSource.filePath(new File(dataFrameInfo.path))
        case _: ExcelSource => DataStreamSource.excel(dataFrameInfo.path.toString)
        case _: InputSource => ???
      }

    }

    def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
      dataSetsScalaList.foreach(ds => {
        val dfInfo = ds.getDataFrameInfo(dataFrameName)
        if (dfInfo.nonEmpty) return dfInfo
      })
      None
    }

  }

}
class CloseTracker {
  val count = new AtomicInteger(0)
  val callback: () => Unit = () => count.incrementAndGet()
}

