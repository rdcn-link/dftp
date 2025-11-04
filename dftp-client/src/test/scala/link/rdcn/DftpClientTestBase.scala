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
trait DftpClientTestBase {

}

object DftpClientTestBase {

  // 文件数量配置
  val binFileCount = 3
  val csvFileCount = 3

  val adminUsername = "admin@instdb.cn"
  val adminPassword = "admin001"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "ANONYMOUS"

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

  def getLine(row: Row): String = {
    val delimiter = ","
    row.toSeq.map(_.toString).mkString(delimiter) + '\n'
  }

}

