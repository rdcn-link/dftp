package link.rdcn.dacp.optree

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import link.rdcn.struct.DataFrame
import org.json.JSONObject

import java.io.File
import java.nio.file.Paths
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
  def executeOperator(functionId: String, inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame
}

class RepositoryClient(host: String = "localhost", port: Int = 8088) extends OperatorRepository{
  override def executeOperator(functionId: String, inputs: Seq[DataFrame],  ctx: FlowExecutionContext): DataFrame = {
    val operatorDir = ctx.fairdHome
    val downloadFuture = downloadPackage(functionId, operatorDir)
    Await.result(downloadFuture, 30.seconds)
    val infoFuture = getOperatorInfo(functionId)
    val info = Await.result(infoFuture, 30.seconds)
    val fileName = info.get("packageName").toString
    val filePath = Paths.get(operatorDir, fileName).toString()
    val functionName = info.get("functionName").toString
    info.get("type") match {
      case LangTypeV2.JAVA_JAR.name =>
        val op = JavaJar(filePath, functionName)
        op.applyToDataFrames(inputs, ctx)
      case LangTypeV2.CPP_BIN.name =>
        val op = CppBin(filePath)
        op.applyToDataFrames(inputs, ctx)
      case LangTypeV2.PYTHON_BIN.name =>
        val op = PythonBin(functionName,filePath)
        op.applyToDataFrames(inputs, ctx)
      case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${info.get("type")}")

    }
  }

  val baseUrl = s"http://$host:$port"

  def getOperatorInfo(functionId: String): Future[JSONObject] = {
    implicit val system: ActorSystem = ActorSystem("HttpClient")
    val downloadUrl = s"$baseUrl/fileInfo?id=$functionId"
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


  def uploadPackage(filePath: String, functionId: String, fileType: String, desc: String, functionName: String): Future[String] = {
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


  def downloadPackage(functionId: String, targetPath: String = ""): Future[Unit] = {
    implicit val system: ActorSystem = ActorSystem("HttpClient")
    val infoFuture = getOperatorInfo(functionId)
    val info = Await.result(infoFuture, 30.seconds)

    val downloadUrl = s"$baseUrl/downloadPackage?id=$functionId"
    val outputFilePath = Paths.get(targetPath, info.get("packageName").asInstanceOf[String]).toString // 下载文件保存路径

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

}
