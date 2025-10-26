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
                             useTls: Boolean = false,
                             protocolScheme: String = "dftp",
                             tlsCertFile: Option[File] = None,
                             tlsKeyFile: Option[File] = None,
                             pubKeyMap: Map[String, PublicKey] = Map.empty,
                             privateKey: Option[PrivateKey] = None) {
}
