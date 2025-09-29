package link.rdcn.log

import link.rdcn.DftpConfig
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.core.config.builder.api.{ConfigurationBuilder, ConfigurationBuilderFactory}
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 23:20
 * @Modified By:
 */
class FileLogger(config: DftpConfig) extends Logger {

  private val logger = {
    val builder: ConfigurationBuilder[BuiltConfiguration] = ConfigurationBuilderFactory.newConfigurationBuilder()

    builder.setStatusLevel(Level.WARN)
    builder.setConfigurationName("FairdLogConfig")

    val logFile = config.logFilePath
    val level = Level.toLevel(config.rootLogLevel)
    val consolePattern = config.consoleLogPattern
    val filePattern = config.fileLogPattern

    val console = builder.newAppender("Console", "CONSOLE")
      .add(builder.newLayout("PatternLayout").addAttribute("pattern", consolePattern))
    builder.add(console)

    val file = builder.newAppender("File", "FILE")
      .addAttribute("fileName", logFile)
      .add(builder.newLayout("PatternLayout").addAttribute("pattern", filePattern))
    builder.add(file)

    builder.add(
      builder.newRootLogger(level)
        .add(builder.newAppenderRef("Console"))
        .add(builder.newAppenderRef("File"))
    )

    Configurator.initialize(builder.build())

    val logger = LogManager.getLogger(getClass)
    logger
  }

  override def info(message: String): Unit =  logger.info(message)

  override def warn(message: String): Unit = logger.warn(message)

  override def error(message: String): Unit = logger.error(message)

  override def debug(message: String): Unit = logger.debug(message)

  override def error(message: String, e: Throwable): Unit = logger.error(message, e)
}
