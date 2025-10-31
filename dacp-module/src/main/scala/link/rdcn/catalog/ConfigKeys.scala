package link.rdcn.catalog

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 14:11
 * @Modified By:
 */
object ConfigKeys {

  val FAIRD_HOME = "faird.home"

  // === 自定义配置项 ===
  val FAIRD_HOST_NAME = "faird.host.name"
  val FAIRD_HOST_TITLE = "faird.host.title"
  val FAIRD_HOST_POSITION = "faird.host.position"
  val FAIRD_HOST_DOMAIN = "faird.host.domain"
  val FAIRD_HOST_PORT = "faird.host.port"

  // === 日志配置 ===
  val LOGGING_FILE_NAME = "logging.file.name"
  val LOGGING_LEVEL_ROOT = "logging.level.root"
  val LOGGING_PATTERN_CONSOLE = "logging.pattern.console"
  val LOGGING_PATTERN_FILE = "logging.pattern.file"

  // === 网络安全配置 ===
  val FAIRD_TLS_ENABLED = "faird.tls.enabled"
  val FAIRD_TLS_CERT_PATH = "faird.tls.cert.path"
  val FAIRD_TLS_KEY_PATH = "faird.tls.key.path"

  val FAIRD_PUBLIC_KEY_PATH = "faird.public.key.path"
  val FAIRD_PRIVATE_KEY_PATH = "faird.private.key.path"

  // === 运行时环境变量配置 ===

  val PYTHON_HOME = "python.home"
}
