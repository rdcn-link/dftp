package link.rdcn.log

import link.rdcn.DftpConfig

import java.io.{BufferedWriter, File, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.ReentrantLock

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/11 17:35
 * @Modified By:
 */
class FileAccessLogger(config: DftpConfig) extends AccessLogger {
  private val lock = new ReentrantLock()
  private var writer: BufferedWriter = _
  private val fileDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private def initWriter(): Unit = {
    val today = LocalDate.now().format(fileDateFormatter)
    val logFile = new File(s"${config.accessLogPath}.$today")

    if (writer == null || !logFile.exists()) {
      closeWriter()
      logFile.getParentFile.mkdirs()
      writer = new BufferedWriter(
        new OutputStreamWriter(
          new FileOutputStream(logFile, true),
          StandardCharsets.UTF_8
        ),
        8192
      )
    }
  }

  private def closeWriter(): Unit = {
    if (writer != null) {
      try {
        writer.flush()
        writer.close()
      } catch {
        case e: IOException =>
          System.err.println(s"Error closing access log writer: ${e.getMessage}")
      }
      writer = null
    }
  }

  private def writeLogEntry(logEntry: String): Unit = {
    lock.lock()
    try {
      initWriter()
      writer.write(logEntry)
      writer.newLine()
      writer.flush()
    } catch {
      case e: IOException =>
        System.err.println(s"Access log write failed: ${e.getMessage}")
    } finally {
      lock.unlock()
    }
  }

  override def logAccess(
                          clientIp: String,
                          user: String,
                          timestamp: String,
                          method: String,
                          path: String,
                          status: Int,
                          responseSize: Long,
                          durationMs: Long,
                          userAgent: Option[String] = None
                        ): Unit = {
    val ua = userAgent.getOrElse("-")
    val logEntry =
      s"""$clientIp - $user [$timestamp] "$method $path" $status $responseSize "$ua" ${durationMs}ms""".stripMargin
    writeLogEntry(logEntry)
  }

  override def logError(
                         clientIp: String,
                         user: String,
                         timestamp: String,
                         method: String,
                         path: String,
                         status: Int,
                         errorMessage: String,
                         durationMs: Long,
                         userAgent: Option[String] = None
                       ): Unit = {
    val ua = userAgent.getOrElse("-")
    val logEntry =
      s"""$clientIp - $user [$timestamp] "$method $path" $status 0 "$ua" ${durationMs}ms error="$errorMessage""".stripMargin
    writeLogEntry(logEntry)
  }

  override def close(): Unit = {
    lock.lock()
    try {
      closeWriter()
    } finally {
      lock.unlock()
    }
  }
}
