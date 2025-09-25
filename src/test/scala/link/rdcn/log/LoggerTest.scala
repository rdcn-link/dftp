package link.rdcn.log

import link.rdcn.DftpConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, Test}

import java.io.File


object LoggerTest {
  @AfterAll
  def cleanup(): Unit = {
    val file = new File("test.log")
    if (file.exists()) {
      file.deleteOnExit()
      println(s"Deleted directory: ${file.getAbsoluteFile}")
    }
  }
}

class LoggerTest extends Logging {
  val mockLogger = new Logger {
    var capturedMessage: String = _
    var capturedThrowable: Throwable = _

    override def info(message: String): Unit = ???

    override def warn(message: String): Unit = ???

    override def error(message: String): Unit = ???

    override def debug(message: String): Unit = ???

    override def error(message: String, e: Throwable): Unit = {
      capturedMessage = message
      capturedThrowable = e
    }
  }

  //测试Logger能否正确运行
  @Test
  def testLoggerTraitDefaultErrorMethod(): Unit = {
    val testException = new RuntimeException("Test Error Message")
    mockLogger.error(testException)
    assertEquals("Test Error Message", mockLogger.capturedMessage, "Captured message should be throwable's message")
    assertEquals(mockLogger.capturedThrowable, testException, "Captured throwable should be the input exception")
  }

  @Test
  def testLoggerFactoryCreateFileLogger(): Unit = {
    val config = TestDftpConfig("file")
    LoggerFactory.setDftpConfig(config)
    val logger = LoggerFactory.createLogger()
    assertTrue(logger.isInstanceOf[FileLogger], "Logger should be an instance of FileLogger")
  }

  //LoggerFactory异常测试
  @Test
  def testLoggerFactoryUnsupportedLoggerType(): Unit = {
    val config = TestDftpConfig("unsupported")
    LoggerFactory.setDftpConfig(config)
    val exception = assertThrows(
      classOf[IllegalArgumentException], () => LoggerFactory.createLogger())
    assertEquals(exception.getMessage, "Unsupported logger type")
  }

  //FileLogger测试
  @Test
  def testFileLoggerAllMethodsCallLog4j(): Unit = {
    val config = TestDftpConfig("file")
    // 覆盖 FileLogger 的构造函数 (初始化 Log4j 配置)
    val fileLogger = new FileLogger(config)
    // 覆盖 FileLogger 的 info/warn/error/debug 方法
    fileLogger.info("Test info message")
    fileLogger.warn("Test warn message")
    fileLogger.error("Test error message")
    fileLogger.debug("Test debug message")
    // 覆盖 error(message, e) 方法
    val testException = new Exception("Error with cause")
    fileLogger.error("Test error with exception", testException)
  }

  //测试Logger懒加载
  @Test
  def testLoggingTraitLazyInitialization(): Unit = {
    val dummyConfig = TestDftpConfig("file")
    LoggerFactory.setDftpConfig(dummyConfig)
    val testInstance = new TestClassWithLogging(dummyConfig)
    val firstLogger = testInstance.getLogger
    val secondAccessLogger = testInstance.getLogger
    assertEquals(firstLogger, secondAccessLogger, "Logger should be the same instance (lazy val check)")
  }

}

//用于测试Logging Trait
class TestClassWithLogging(config: DftpConfig) extends Logging {
  def getLogger: Logger = logger
}

case class TestDftpConfig(newLoggerType: String) extends DftpConfig {
  override val loggerType: String = newLoggerType
  override val rootLogLevel: String = "debug"
  override val logFilePath: String = "test.log"
  override val consoleLogPattern: String = "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"
  override val fileLogPattern: String = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"

  override def host: String = ???

  override def port: Int = ???
}

