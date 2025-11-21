package link.rdcn.dacp.optree

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import cn.cnic.operatordownload.client.OperatorClient
import link.rdcn.dacp.optree.fifo.{DockerContainer, FileType}
import link.rdcn.dacp.recipe.FifoFileBundleFlowNode
import link.rdcn.dacp.utils.FileUtils
import link.rdcn.struct.DataFrame
import org.json.{JSONArray, JSONObject}

import java.io.File
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/30 17:03
 * @Modified By:
 */
trait OperatorRepository {
  def executeOperator(functionName: String, functionVersion: Option[String], inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame

  def parseTransformFunctionWrapper(functionName: String, functionVersion: Option[String], ctx: FlowExecutionContext): TransformFunctionWrapper
}

class RepositoryClient(host: String = "http://10.0.89.39", port: Int = 8090) extends OperatorRepository {

  override def parseTransformFunctionWrapper(functionName: String, functionVersion: Option[String], ctx: FlowExecutionContext): TransformFunctionWrapper = {
    val client: OperatorClient = OperatorClient.connect(s"$host:$port", null)
    val operatorInfo = new JSONObject(client.getOperatorByNameAndVersion(functionName, functionVersion.orNull))
    if (operatorInfo.has("data") && operatorInfo.getJSONObject("data").getString("type") == "python-script") {
      val operatorImage = operatorInfo.getJSONObject("data").getString("nexusUrl")

      val inputCounter = new AtomicLong(0)
      val outputCounter = new AtomicLong(0)
      val ja = new JSONArray(operatorInfo.getJSONObject("data").getString("paramInfos"))
      val files = (0 until ja.length).map(index => ja.getJSONObject(index))
        //subfix,fileType,inParam,paramType
        .map(jo => (jo.getString("name"), jo.getString("fileType"), jo.getString("paramDescription"), jo.getString("paramType")))
        .map(file => {
          if (file._4 == "INPUT_FILE") {
            (file._1, file._2, file._3, file._4, s"input${inputCounter.incrementAndGet()}${file._1}")
          } else {
            (file._1, file._2, file._3, file._4, s"output${outputCounter.incrementAndGet()}${file._1}")
          }
        })
      val commands = operatorInfo.getJSONObject("data").getString("command").split(" ")

      val operationId = s"${functionName}_${UUID.randomUUID().toString}"
      val hostPath = FileUtils.getTempDirectory("", operationId)
      val containerPath = s"/$operationId"

      val commandsWithParams = commands ++ files.flatMap(file => Seq(file._3, Paths.get(containerPath, file._5).toString))

      val inputFiles = files.filter(_._4 == "INPUT_FILE")
        .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
      val outputFiles = files.filter(_._4 == "OUTPUT_FILE")
        .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
      val dockerContainer = DockerContainer(functionName, Some(hostPath), Some(containerPath), Some(operatorImage))
      FileRepositoryBundle(commandsWithParams, inputFiles, outputFiles, dockerContainer)
    } else {
      val operatorDir = ctx.fairdHome
      val downloadFuture = downloadPackage(functionName, functionVersion.getOrElse("1.0.0"), operatorDir)
      Await.result(downloadFuture, 30.seconds)
      val infoFuture = getOperatorInfo(functionName, functionVersion.getOrElse("1.0.0"))
      val info = Await.result(infoFuture, 30.seconds)
      val filePath = sanitizeArtifact(Paths.get(operatorDir,info.getString("packageName")))
      val fileName = filePath.getFileName.toString
      val operatorFunctionName = info.get("functionName").toString
      info.get("type") match {
        case LangTypeV2.JAVA_JAR.name =>
          JavaJar(filePath.toString, operatorFunctionName)
        case LangTypeV2.CPP_BIN.name =>
          CppBin(filePath.toString)
        case LangTypeV2.PYTHON_BIN.name =>
          PythonBin(operatorFunctionName, fileName)
        case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${info.get("type")}")

      }
    }
  }


  override def executeOperator(functionId: String, functionVersion: Option[String],
                               inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    val operatorInfo = new JSONObject(client.getOperatorByNameAndVersion(functionId, functionVersion.getOrElse(null)))
    if (operatorInfo.getJSONObject("data").getString("type") == "python-script") {
      val operatorImage = operatorInfo.getJSONObject("data").getString("nexusUrl")

      val inputCounter = new AtomicLong(0)
      val outputCounter = new AtomicLong(0)
      val ja = new JSONArray(operatorInfo.getJSONObject("data").getString("paramInfos"))
      val files = (0 until ja.length).map(index => ja.getJSONObject(index))
        //subfix,fileType,inParam,paramType
        .map(jo => (jo.getString("name"), jo.getString("fileType"), jo.getString("paramDescription"), jo.getString("paramType")))
        .map(file => {
          if (file._4 == "INPUT_FILE") {
            (file._1, file._2, file._3, file._4, s"input${inputCounter.incrementAndGet()}${file._1}")
          } else {
            (file._1, file._2, file._3, file._4, s"output${outputCounter.incrementAndGet()}${file._1}")
          }
        })
      val commands = operatorInfo.getJSONObject("data").getString("command").split(" ")

      val nodeGullySlopId = s"${functionId}_${UUID.randomUUID().toString}"
      val hostPath = FileUtils.getTempDirectory("", nodeGullySlopId)
      val containerPath = s"/$nodeGullySlopId"

      val commandsWithParams = commands ++ files.flatMap(file => Seq(file._3, Paths.get(containerPath, file._5).toString))

      val inputFiles = files.filter(_._4 == "INPUT_FILE")
        .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
      val outputFiles = files.filter(_._4 == "OUTPUT_FILE")
        .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
      val dockerContainer = DockerContainer(functionId, Some(hostPath), Some(containerPath), Some(operatorImage))
      val op = FifoFileBundleFlowNode(commandsWithParams, inputFiles, outputFiles, dockerContainer)
      DataFrame.empty()
    } else {
      val operatorDir = ctx.fairdHome
      val downloadFuture = downloadPackage(functionId, functionVersion.getOrElse("1.0.0"), operatorDir)
      Await.result(downloadFuture, 30.seconds)
      val infoFuture = getOperatorInfo(functionId, functionVersion.getOrElse("1.0.0"))
      val info = Await.result(infoFuture, 30.seconds)
      val fileName = sanitizeArtifact(Paths.get(operatorDir,info.getString("packageName")))
      val filePath = Paths.get(operatorDir, fileName.toString).toString()
      val functionName = info.get("functionName").toString
      info.get("type") match {
        case LangTypeV2.JAVA_JAR.name =>
          val op = JavaJar(filePath, functionName)
          op.applyToDataFrames(inputs, ctx)
        case LangTypeV2.CPP_BIN.name =>
          val op = CppBin(filePath)
          op.applyToDataFrames(inputs, ctx)
        case LangTypeV2.PYTHON_BIN.name =>
          val op = PythonBin(functionName, filePath)
          op.applyToDataFrames(inputs, ctx)
        case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${info.get("type")}")
      }
    }
  }

  val baseUrl = s"$host:$port"

  def getOperatorInfo(functionId: String, version: String = "1.0.0"): Future[JSONObject] = {
    implicit val system: ActorSystem = ActorSystem("HttpClient")
    val downloadUrl = s"$baseUrl/fileInfo?id=$functionId&version=$version"
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = downloadUrl
    )

    val resultFuture: Future[JSONObject] = Http().singleRequest(request).flatMap { response =>
      if (response.status.isSuccess()) {

        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { jsonString =>
          try {
            val jsonObject = new JSONObject(jsonString)
            Future.successful(jsonObject)
          } catch {
            case ex: Exception =>
              Future.failed(new RuntimeException(s"JSON parsing faild: ${ex.getMessage}. body: $jsonString", ex))
          }
        }
      } else {
        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { body =>
          Future.failed(new RuntimeException(s"Request failed，Status: ${response.status}, body: $body"))
        }.recoverWith {
          case ex: Exception => Future.failed(new RuntimeException(s"Request failed，Status: ${response.status}, cannot get body: ${ex.getMessage}", ex))
        }
      }
    }
    resultFuture.andThen {
      case _ =>
        system.terminate()
    }
  }


  def uploadPackage(filePath: String, functionId: String, fileType: String, desc: String, functionName: String, version: String = "1.0.0"): Future[String] = {
    implicit val system: ActorSystem = ActorSystem("HttpClient")
    val file = new File(filePath)

    if (!file.exists()) {
      Future.failed(new IllegalArgumentException(s"File does not exist: $filePath"))
    }

    // 创建文件上传的 ByteString 源
    val fileSource = FileIO.fromPath(file.toPath)

    // 构建 multipart/form-data
    val formData = Multipart.FormData(
      Source(
        List(
          // 'id' 字段
          Multipart.FormData.BodyPart.Strict(
            "id",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(functionId))
          ),
          // 'file' 字段，包含文件内容和文件名
          Multipart.FormData.BodyPart(
            "file",
            HttpEntity(ContentTypes.`application/octet-stream`, file.length(), fileSource),
            Map("filename" -> file.getName) // 设置文件名
          ),
          // 'type' 字段
          Multipart.FormData.BodyPart.Strict(
            "type",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(fileType))
          ),
          // 'desc' 字段
          Multipart.FormData.BodyPart.Strict(
            "desc",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(desc))
          ),
          // 'functionName' 字段
          Multipart.FormData.BodyPart.Strict(
            "functionName",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(functionName))
          ),
          // 'version' 字段
          Multipart.FormData.BodyPart.Strict(
            "version",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(version))
          )
        )
      )
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"$baseUrl/uploadPackage",
      entity = formData.toEntity()
    )
    val resultFuture: Future[String] = Http().singleRequest(request).flatMap { response =>
      if (response.status.isSuccess()) {
        response.entity.toStrict(5.seconds).map(_.data.utf8String)
        Future.successful("success")
      } else {
        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { body =>
          Future.failed(new RuntimeException(s"Upload failed，Status: ${response.status}, body: $body"))
        }.recoverWith {
          case ex: Exception => Future.failed(new RuntimeException(s"Upload failed，Status: ${response.status}, cannot get body: ${ex.getMessage}", ex))
        }
      }
    }
    resultFuture.andThen {
      case _ =>
        system.terminate()
    }
  }


  def downloadPackage(functionName: String, functionVersion: String = "1.0.0", targetPath: String = ""): Future[Unit] = {
    implicit val system: ActorSystem = ActorSystem("HttpClient")
    val infoFuture = getOperatorInfo(functionName, functionVersion)
    val info = Await.result(infoFuture, 30.seconds)

    val downloadUrl = s"$baseUrl/downloadPackage?id=$functionName&version=$functionVersion"

    val outputFilePath = sanitizeArtifact(Paths.get(targetPath,info.getString("packageName"))).toString

    // 创建 HTTP GET 请求
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = downloadUrl
    )

    // 发送请求并处理响应
    val resultFuture: Future[Unit] = Http().singleRequest(request).flatMap { response =>
      if (response.status.isSuccess()) {
        val outputFile = new File(outputFilePath)
        val fileSink = FileIO.toPath(outputFile.toPath)

        response.entity.dataBytes.runWith(fileSink).map(_ => ()).andThen { // map to Unit, andThen for side effects
          case Success(_) => println(s"Download success: ${outputFile.getAbsolutePath}")
          case Failure(ex) =>
            println(s"Data write to file failed: ${ex.getMessage}")
            response.discardEntityBytes() // 确保丢弃未消费的实体字节
        }
      } else {
        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { body =>
          Future.failed(new RuntimeException(s"Download failed，Status: ${response.status}, body: $body"))
        }.recoverWith {
          case ex: Exception => Future.failed(new RuntimeException(s"Download failed，Status: ${response.status}, cannot get body: ${ex.getMessage}", ex))
        }
      }
    }
    resultFuture.andThen {
      case _ =>
        system.terminate()
    }
  }

  import java.nio.file.{Files, Path, Paths, StandardCopyOption}
  import scala.util.matching.Regex

    // 定义正则：捕获 (基础名) (连字符+版本号) (.whl后缀)
    // 说明：
    // 1. (.+) 贪婪匹配，会尽可能往后吃，直到剩下必须满足后面条件的部分
    // 2. (-\d+(?:\.\d+)+) 匹配末尾的类似 -2.0.0 的结构
    // 3. (\.whl) 匹配后缀
    private val WhlRedundantVersionPattern: Regex = """(.+)(-\d+(?:\.\d+)+)(\.whl)$""".r

    /**
     * 规范化文件名称
     *
     * @param originalPath 下载后的原始文件路径
     * @return 处理后的文件路径（如果是 whl 则为新路径，否则为原路径）
     */
    def sanitizeArtifact(originalPath: Path): Path = {
      val fileName = originalPath.getFileName.toString

      // 仅处理 .whl 文件，忽略 jar 和 cpp/exe
      if (fileName.endsWith(".whl")) {
        fileName match {
          case WhlRedundantVersionPattern(baseName, redundantVersion, extension) =>
            // 拼接符合 PEP 427 规范的新名称
            val newFileName = baseName + extension
            originalPath.resolveSibling(newFileName)
          case _ =>
            // 正则未匹配（说明文件名本身没带额外的 -2.0.0 后缀），保持原样
            originalPath
        }
      } else {
        // 非 whl 文件直接返回
        originalPath
      }
    }
//
//  def replaceVersionId(functionVersion: String, info: JSONObject, targetPath: String): String = {
//    val extension = ".whl"
//    val suffixToRemove = s"-$functionVersion"
//    val fixedFilename = info.get("packageName").asInstanceOf[String]
//      .stripSuffix(extension)
//      .stripSuffix(suffixToRemove) + extension
//    Paths.get(targetPath, fixedFilename).toString // 下载文件保存路径
//  }

}
