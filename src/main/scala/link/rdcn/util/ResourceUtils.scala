package link.rdcn.util

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 17:55
 * @Modified By:
 */
object ResourceUtils {
  def using[A <: AutoCloseable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }

  def usingAll(resources: AutoCloseable*)(block: => Unit): Unit = {
    try block
    finally {
      resources.reverse.foreach { res =>
        try if (res != null) res.close()
        catch {
          case ex: Throwable => println(s"Warning: Failed to close resource: ${ex.getMessage}")
        }
      }
    }
  }

}
