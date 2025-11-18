package link.rdcn.server

import java.beans.BeanProperty
import java.io.File
import java.security.{PrivateKey, PublicKey}
import scala.collection.convert.ImplicitConversions.`map AsScala`

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/11 19:24
 * @Modified By:
 */
class DftpServerConfigBean {
  @BeanProperty var host: String = _
  @BeanProperty var port: Int = 3101
  @BeanProperty var dftpHome: String = _
  @BeanProperty var dftpDataSource: String = _
  @BeanProperty var useTls: Boolean = false
  @BeanProperty var protocolScheme: String = "dftp"
  @BeanProperty var tlsCertFile: File = _
  @BeanProperty var tlsKeyFile: File = _
  @BeanProperty var pubKeyMap: java.util.Map[String, PublicKey] = new java.util.HashMap()
  @BeanProperty var privateKey: PrivateKey = _
  @BeanProperty var modules: ModulesBean = _

  def toDftpServerConfig: DftpServerConfig = {
    DftpServerConfig(
      host = host,
      port = port,
      dftpHome = Option(dftpHome),
      dftpDataSource = Option(dftpDataSource),
      useTls = useTls,
      protocolScheme = protocolScheme,
      tlsCertFile = Option(tlsCertFile),
      tlsKeyFile = Option(tlsKeyFile),
      pubKeyMap = if (pubKeyMap != null) pubKeyMap.toMap else Map.empty,
      privateKey = Option(privateKey)
    )
  }
}
