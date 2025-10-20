package link.rdcn.server

import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/27 09:43
 * @Modified By:
 */
trait GetRequest {
  def getRequestURI(): String

  def getRequestURL(): String

  def getUserPrincipal(): UserPrincipal
}

trait GetResponse {
  def sendDataFrame(dataFrame: DataFrame): Unit

  def sendError(code: Int, message: String): Unit
}

trait ActionRequest {
  def getActionName(): String

  def getParameter(): Array[Byte]

  def getParameterAsMap(): Map[String, Any]

  def getUserPrincipal(): UserPrincipal
}

trait ActionResponse {
  def send(data: Array[Byte]): Unit

  def sendError(code: Int, message: String): Unit
}

trait PutRequest {
  def getDataFrame(): DataFrame
}

trait PutResponse {
  def send(data: Array[Byte]): Unit

  def sendError(code: Int, message: String): Unit
}
