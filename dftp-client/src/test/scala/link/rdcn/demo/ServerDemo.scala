/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 17:04
 * @Modified By:
 */
package link.rdcn.demo

import link.rdcn.DftpConfig
import link.rdcn.ClientTestBase.doListHostInfo
import link.rdcn.ClientTestDemoProvider.{authProvider, dataProvider}
import link.rdcn.server.{ActionRequest, ActionResponse, DftpMethodService, DftpServer, GetRequest, GetResponse, PutRequest, PutResponse}
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}
import link.rdcn.util.CodecUtils
import org.json.JSONObject

object ServerDemo {
  def main(args: Array[String]): Unit = {
    /**
     * server配置
     * 至少给出host和port
     */
    val dftpConfig = new DftpConfig() {
      override val rootLogLevel: String = "debug"
      override val consoleLogPattern: String = "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"
      override val fileLogPattern: String = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"

      override def host: String =
          "localhost"

        override def port: Int =
          3101
      }

    /**
     * 实现用户登录状态凭证
     */
    class AuthenticatedUser(userName: String, token: String) extends UserPrincipal

    /**
     * 实现用户认证服务
     * 登录成功时返回用户登录状态凭证，失败时抛出异常
     */
    val authenticationService = new AuthenticationService {
      override def authenticate(credentials: Credentials): UserPrincipal = {
        new AuthenticatedUser("","")
      }
    }

    /**
     * 实现server数据处理和传输服务
     */
    val dftpMethodService: DftpMethodService = new DftpMethodService {
      /**
       * client向server请求数据
       * 还可以自定义的信息
       * @param request
       * @param response
       */
      override def doGet(request: GetRequest, response: GetResponse): Unit = {
        request.getRequestURI() match {
          //自定义请求实现
          case "/listHostResourceInfo" => {
            try {
              response.sendDataFrame(doListHostInfo)
            } catch {
              case e: Exception =>
                response.sendError(500, e.getMessage)
            }
          }
          case otherPath =>
            //数据请求实现
            val userPrincipal = request.getUserPrincipal()
            //实现authProvider用于数据鉴权
            if(authProvider.checkPermission(userPrincipal, otherPath)){
              //实现dataProvider用于从url中解析DataStreamSource
              val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(otherPath)
              val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
              response.sendDataFrame(dataFrame)
            }else{
              response.sendError(403, s"access dataFrame $otherPath Forbidden")
            }
        }
      }

      /**
       * client向server上传数据
       * @param request
       * @param response
       */
      override def doPut(request: PutRequest, response: PutResponse): Unit = {
        val dataFrame = request.getDataFrame()
        try {
          //        对接收到的数据做处理
          //        dataReceiver.receive(dataFrame)
        } catch {
          case e: Exception => response.sendError(500, e.getMessage)
        }
        response.send(CodecUtils.encodeString(new JSONObject().put("status","success").toString))
      }

      /**
       * client向server请求执行其他自定义操作
       * 一般不进行实现
       * @param request
       * @param response
       */
      override def doAction(request: ActionRequest, response: ActionResponse): Unit =
        response.sendError(501, s"${request.getActionName()} Not Implemented")
    }

    /**
     * server实例化
     * 需要给出AuthenticationService和DftpMethodService的实现、服务器配置
     * 使用setProtocolSchema指定协议名
     * 使用enableTLS，给出服务器证书和密钥启用tls加密连接
     */
    val server = new DftpServer(authenticationService, dftpMethodService)
    server.setProtocolSchema("dftp")
//    server.enableTLS(tlsCertFile, tlsKeyFile)
    server.start(dftpConfig)
  }



}
