package link.rdcn.client.dacp

import link.rdcn.client.{DacpClient, UrlValidator}
import link.rdcn.user.Credentials

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/9 17:55
 * @Modified By:
 */
class DacpClientProxy private(host: String, port: Int, useTLS: Boolean = false) extends DacpClient(host, port, useTLS) {
  def getTargetServerUrl: String = {
    new String(doAction("/getTargetServerUrl"), "UTF-8")
  }
}


object DacpClientProxy {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClientProxy = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClientProxy(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClientProxy = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClientProxy(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
