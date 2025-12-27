package link.rdcn.server

import link.rdcn.message.MapSerializer
import link.rdcn.operation.TransformOp
import link.rdcn.struct.{Blob, DataFrame, DataFrameInfo}
import link.rdcn.user.UserPrincipal
import org.json.JSONObject

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/27 09:43
 * @Modified By:
 */
trait DftpRequest {
  val attributes = mutable.Map[String, Any]()

  def getUserPrincipal(): UserPrincipal
}

trait DftpActionRequest extends DftpRequest {
  def getActionName(): String
  def getRequestParameters(): JSONObject
}

trait DftpPutStreamRequest extends DftpRequest {
  def getDataFrame(): DataFrame
}

trait DftpResponse {
  def sendError(errorCode: Int, message: String): Unit
}

trait DftpPlainResponse extends DftpResponse {
  def sendData(data: Array[Byte])
  def sendData(map: Map[String, Any]): Unit = sendData(MapSerializer.encodeMap(map))
}

trait DftpActionResponse extends DftpResponse {
  def sendRedirect(dataFrameContext: DataFrameContext)
  def sendRedirect(blobContext: BlobContext)
  def sendJsonString(json: String)
  def sendJsonObject(json: JSONObject) = sendJsonString(json.toString)
}

trait DftpPutStreamResponse extends DftpPlainResponse