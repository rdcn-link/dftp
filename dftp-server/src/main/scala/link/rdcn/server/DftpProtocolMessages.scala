package link.rdcn.server

import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/27 09:43
 * @Modified By:
 */
trait DftpRequest

trait DftpResponse

trait GetRequest extends DftpRequest{
  def getRequestURI(): String

  def getRequestURL(): String

  def getUserPrincipal(): UserPrincipal
}

trait GetResponse extends DftpResponse {
  def sendDataFrame(dataFrame: DataFrame): Unit

  def sendError(code: Int, message: String): Unit
}

trait ActionRequest extends DftpRequest {
  def getActionName(): String

  def getParameter(): Array[Byte]

  def getParameterAsMap(): Map[String, Any]

  def getUserPrincipal(): UserPrincipal
}

trait ActionResponse extends DftpResponse {
  def send(data: Array[Byte]): Unit

  def sendError(code: Int, message: String): Unit
}

trait PutRequest extends DftpRequest {
  def getDataFrame(): DataFrame
}

trait PutResponse extends DftpResponse {
  def send(data: Array[Byte]): Unit

  def sendError(code: Int, message: String): Unit
}
