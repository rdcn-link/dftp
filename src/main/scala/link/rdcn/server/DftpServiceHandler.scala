package link.rdcn.server

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/26 17:14
 * @Modified By:
 */
trait DftpServiceHandler {

  def doGet(request: GetRequest, response: GetResponse): Unit

  def doPut(request: PutRequest, putResponse: PutResponse): Unit

  def doAction(request: ActionRequest, response: ActionResponse): Unit

}
