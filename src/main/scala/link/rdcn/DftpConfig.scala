package link.rdcn

import java.io.File
import java.security.{PrivateKey, PublicKey}

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/28 15:10
 * @Modified By:
 */
trait DftpConfig {
  def host: String

  def port: Int

  def pubKeyMap: Map[String, PublicKey] = Map.empty

  def privateKey: Option[PrivateKey] = None

  def logFilePath: String = "./dftp.log"

  def rootLogLevel: String = "INFO"

  def consoleLogPattern: String = "%d{HH:mm:ss} %-5level %logger{36} - %msg%n"

  def fileLogPattern: String = "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n"
}
