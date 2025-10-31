package link.rdcn.server
import java.io.File
import java.security.{PrivateKey, PublicKey}

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/28 15:10
 * @Modified By:
 */
case class DftpServerConfig(
                             host: String,
                             port: Int = 3101,
                             dftpHome: Option[String] = None,
                             dftpDataSource: Option[String] = None,
                             useTls: Boolean = false,
                             protocolScheme: String = "dftp",
                             tlsCertFile: Option[File] = None,
                             tlsKeyFile: Option[File] = None,
                             pubKeyMap: Map[String, PublicKey] = Map.empty,
                             privateKey: Option[PrivateKey] = None) {

  def withHost(newHost: String): DftpServerConfig = this.copy(host = newHost)

  def withPort(newPort: Int): DftpServerConfig = this.copy(port = newPort)

  def withDftpHome(newDftpHome: Option[String]): DftpServerConfig = this.copy(dftpHome = newDftpHome)

  def withDftpHome(newDftpHome: String): DftpServerConfig = this.copy(dftpHome = Some(newDftpHome))

  def withDataSourcePath(newDataPath: String): DftpServerConfig = this.copy(dftpDataSource = Some(newDataPath))

  def withUseTls(newUseTls: Boolean): DftpServerConfig = this.copy(useTls = newUseTls)

  def withProtocolScheme(newProtocolScheme: String): DftpServerConfig = this.copy(protocolScheme = newProtocolScheme)

  def withTlsCertFile(newTlsCertFile: Option[File]): DftpServerConfig = this.copy(tlsCertFile = newTlsCertFile)

  def withTlsKeyFile(newTlsKeyFile: Option[File]): DftpServerConfig = this.copy(tlsKeyFile = newTlsKeyFile)

  def withPubKeyMap(newPubKeyMap: Map[String, PublicKey]): DftpServerConfig = this.copy(pubKeyMap = newPubKeyMap)

  def withPrivateKey(newPrivateKey: Option[PrivateKey]): DftpServerConfig = this.copy(privateKey = newPrivateKey)

  def withTlsCertFile(newTlsCertFile: File): DftpServerConfig = this.copy(tlsCertFile = Some(newTlsCertFile))

  def withTlsKeyFile(newTlsKeyFile: File): DftpServerConfig = this.copy(tlsKeyFile = Some(newTlsKeyFile))

  def withPrivateKey(newPrivateKey: PrivateKey): DftpServerConfig = this.copy(privateKey = Some(newPrivateKey))

  // Method to add a single public key to the map
  def addPublicKey(name: String, key: PublicKey): DftpServerConfig =
    this.copy(pubKeyMap = pubKeyMap + (name -> key))

  // Method to remove a public key from the map
  def removePublicKey(name: String): DftpServerConfig =
    this.copy(pubKeyMap = pubKeyMap - name)
}
