package link.rdcn

import org.apache.logging.log4j.{LogManager, Logger}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 14:12
 * @Modified By:
 */
trait Logging {
  protected lazy val logger: Logger = LogManager.getLogger(getClass)
}
