package link.rdcn.log

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 23:19
 * @Modified By:
 */
trait Logger {
  def info(message: String): Unit

  def warn(message: String): Unit

  def error(message: String): Unit

  def debug(message: String): Unit

  def error(message: String, e: Throwable): Unit

  def error(e: Throwable): Unit = error(e.getMessage, e)
}