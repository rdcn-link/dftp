package link.rdcn.server

import link.rdcn.message.MapSerializer
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
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

trait DftpGetStreamRequest extends DftpRequest {
  def getTicket(): String
}

trait DftpActionRequest extends DftpRequest {
  def getJsonStringRequest(): String
  def getJonsObjectRequest(): JSONObject = new JSONObject(getJsonStringRequest)
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
  def sendJsonString(json: String)
  def sendJsonObject(json: JSONObject) = sendJsonString(json.toString)
}

trait DftpPutStreamResponse extends DftpPlainResponse

trait DftpGetStreamResponse extends DftpResponse {
  def sendDataFrame(data: DataFrame)
}