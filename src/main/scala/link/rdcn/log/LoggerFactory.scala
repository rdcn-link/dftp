package link.rdcn.log

import link.rdcn.DftpConfig

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 23:24
 * @Modified By:
 */
object LoggerFactory {

  private var config: DftpConfig = _

  def setDftpConfig(dftpConfig: DftpConfig): Unit = config = dftpConfig

  def createLogger(): Logger = {
    config.loggerType match {
      case "file" => new FileLogger(config)
//      case "blockchain" => new BlockchainLogger(config) //支持区块链记账
      case _ => throw new IllegalArgumentException("Unsupported logger type")
    }
  }
}
