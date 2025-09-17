package link.rdcn.server

import link.rdcn.struct.DataFrame
import link.rdcn.user.AuthenticatedUser

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/27 09:43
 * @Modified By:
 */
trait GetRequest {
  def getRequestedPath(): String

  def getRequestAuthenticated(): AuthenticatedUser
}

trait GetResponse {
  def sendDataFrame(dataFrame: DataFrame): Unit

  def sendError(code: Int, message: String): Unit
}

trait ActionRequest {
  def getActionName(): String

  def getActionParameters(): Array[Byte]

  def getActionParameterMap(): Map[String, Any]
}

trait ActionResponse {
  def sendMessage(message: String): Unit

  def sendError(code: Int, message: String): Unit
}

trait PutRequest {
  def getDataFrame(): DataFrame
}

trait PutResponse {
  def sendMessage(message: String): Unit

  def sendError(code: Int, message: String): Unit
}
