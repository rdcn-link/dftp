package link.rdcn.server

import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

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
}

trait DftpGetPathStreamRequest extends DftpGetStreamRequest {
  def getRequestPath(): String
  def getRequestURL(): String
  def getTransformOp(): TransformOp
}

trait DacpGetBlobStreamRequest extends DftpGetStreamRequest {
  def getBlobId(): String
}

trait DftpActionRequest extends DftpRequest {
  def getActionName(): String

  def getParameter(): Array[Byte]

  def getParameterAsMap(): Map[String, Any] = MapSerializer.decodeMap(getParameter())
}

trait DftpPutRequest extends DftpRequest {
  def getDataFrame(): DataFrame
}

trait DftpResponse {
  def sendError(errorCode: Int, message: String): Unit
}

trait DftpPlainResponse extends DftpResponse {
  def sendData(data: Array[Byte])
  def sendData(map: Map[String, Any]): Unit = sendData(MapSerializer.encodeMap(map))
}

trait DftpActionResponse extends DftpPlainResponse

trait DftpPutResponse extends DftpPlainResponse

trait DftpGetResponse extends DftpResponse {
  def sendDataFrame(data: DataFrame)
}