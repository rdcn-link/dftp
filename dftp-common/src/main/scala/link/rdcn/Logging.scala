package link.rdcn

import org.apache.logging.log4j.{LogManager, Logger}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 23:28
 * @Modified By:
 */
trait Logging {
  protected lazy val logger: Logger = LogManager.getLogger(getClass)
}
