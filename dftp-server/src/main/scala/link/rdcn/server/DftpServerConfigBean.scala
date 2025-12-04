package link.rdcn.server

import link.rdcn.util.KeyBasedAuthUtils

import java.io.File
import scala.beans.BeanProperty

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
  @BeanProperty var publicKeyMapPath: String  = _
  @BeanProperty var privateKeyPath: String = _
  @BeanProperty var modules: Array[DftpModule] = Array.empty

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
      pubKeyMap = {
        val pubKeyFile = new File(publicKeyMapPath)
        if(pubKeyFile.exists()) KeyBasedAuthUtils.loadPublicKeys(publicKeyMapPath)
        else Map.empty
      },
      privateKey = {
        val privateKeyFile = new File(privateKeyPath)
        if(privateKeyFile.exists()) Option(KeyBasedAuthUtils.loadPrivateKey(privateKeyPath))
        else None
      }
    )
  }
}
