package link.rdcn.log

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 23:28
 * @Modified By:
 */
trait Logging {
  protected lazy val logger: Logger = LoggerFactory.createLogger
}
