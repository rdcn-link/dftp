package link.rdcn

import com.sun.management.OperatingSystemMXBean
import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}
import org.apache.arrow.flight.CallStatus
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.json.JSONObject

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.{Collections, UUID}
import scala.collection.JavaConverters.seqAsJavaListConverter


/** *
 * 所有测试用相关公共类和变量
 */
trait DacpModuleTestBase {

}

object DacpModuleTestBase {

  // 文件数量配置
  val binFileCount = 3
  val csvFileCount = 3

  val adminUsername = "admin@instdb.cn"
  val adminPassword = "admin001"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "ANONYMOUS"

  //生成Token
  val genToken = () => UUID.randomUUID().toString
  val resourceUrl = getClass.getProtectionDomain.getCodeSource.getLocation
  val testClassesDir = new File(resourceUrl.toURI)
  val demoBaseDir = Paths.get( "src", "test", "resources").toString


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

  def getLine(row: Row): String = {
    val delimiter = ","
    row.toSeq.map(_.toString).mkString(delimiter) + '\n'
  }

  def convertStructTypeToArrowSchema(structType: StructType): Schema = {
    val fields: List[Field] = structType.columns.map { column =>
      val arrowFieldType = column.colType match {
        case IntType =>
          new FieldType(column.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(column.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(column.nullable, ArrowType.Bool.INSTANCE, null)
        case BinaryType =>
          new FieldType(column.nullable, new ArrowType.Binary(), null)
        case RefType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "Url")
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null, metadata)
        case BlobType =>
          val metadata = new java.util.HashMap[String, String]()
          metadata.put("logicalType", "blob")
          new FieldType(column.nullable, new ArrowType.Binary(), null, metadata)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${column.colType}")
      }

      new Field(column.name, arrowFieldType, Collections.emptyList())
    }.toList

    new Schema(fields.asJava)
  }

  def sendErrorWithFlightStatus(code: Int, message: String): UserPrincipal = {
    val status = code match {
      case 400 => CallStatus.INVALID_ARGUMENT
      case 401 => CallStatus.UNAUTHENTICATED
      case 403 => CallStatus.UNAUTHORIZED
      case 404 => CallStatus.NOT_FOUND
      case 408 => CallStatus.TIMED_OUT
      case 409 => CallStatus.ALREADY_EXISTS
      case 500 => CallStatus.INTERNAL
      case 501 => CallStatus.UNIMPLEMENTED
      case 503 => CallStatus.UNAVAILABLE
      case _ => CallStatus.UNKNOWN
    }
    throw status.withDescription(message).toRuntimeException
  }

  def doListHostInfo(): DataFrame = {
    val schema = StructType.empty.add("name", StringType).add("resourceInfo", StringType)
    val hostName = "hostName"
    val stream = Seq((hostName, getHostResourceString()))
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }

  def getHostResourceString(): String = {
    val jo = new JSONObject()
    getResourceStatusString.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }

  def getResourceStatusString(): Map[String, String] = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
    val runtime = Runtime.getRuntime

    val cpuLoadPercent = (osBean.getSystemCpuLoad * 100).formatted("%.2f")
    val availableProcessors = osBean.getAvailableProcessors

    val totalMemory = runtime.totalMemory() / 1024 / 1024 // MB
    val freeMemory = runtime.freeMemory() / 1024 / 1024 // MB
    val maxMemory = runtime.maxMemory() / 1024 / 1024 // MB
    val usedMemory = totalMemory - freeMemory

    val systemMemoryTotal = osBean.getTotalPhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryFree = osBean.getFreePhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryUsed = systemMemoryTotal - systemMemoryFree
    Map(
      "cpu.cores" -> s"$availableProcessors",
      "cpu.usage.percent" -> s"$cpuLoadPercent%",
      "jvm.memory.max.mb" -> s"$maxMemory MB",
      "jvm.memory.total.mb" -> s"$totalMemory MB",
      "jvm.memory.used.mb" -> s"$usedMemory MB",
      "jvm.memory.free.mb" -> s"$freeMemory MB",
      "system.memory.total.mb" -> s"$systemMemoryTotal MB",
      "system.memory.used.mb" -> s"$systemMemoryUsed MB",
      "system.memory.free.mb" -> s"$systemMemoryFree MB"
    )
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

//  object ConfigLoader {
//    var dftpConfig: DftpConfig = _
//
//    def init(): Unit = synchronized {
//      dftpConfig = load()
//    }
//
//    def loadProperties(path: String): Properties = {
//      val props = new Properties()
//      val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
//      try props.load(fis) finally fis.close()
//      props
//    }
//
//    def getConfig(): DftpConfig = dftpConfig
//
//    def load(): DftpConfig = {
//      new DftpConfig() {
//        override def host: String =
//          "localhost"
//
//        override def port: Int =
//          3101
//      }
//    }
//  }

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

  trait AuthProvider extends AuthenticationService{

    /**
     * 用户认证，成功返回认证后的保持用户登录状态的凭证
     */
    def authenticate(credentials: Credentials): UserPrincipal

    /**
     * 判断用户是否具有某项权限
     *
     * @param user          已认证用户
     * @param dataFrameName 数据帧名称
     * @param opList        操作类型列表（Java List）
     * @return 是否有权限
     */
    def checkPermission(user: UserPrincipal,
                        dataFrameName: String,
                        opList: List[Object] = List.empty): Boolean
  }
}

