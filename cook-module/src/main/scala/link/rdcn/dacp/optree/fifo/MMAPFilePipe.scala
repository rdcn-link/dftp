package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._

import java.io.{ByteArrayOutputStream, File, IOException, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class MMAPFilePipe(file: File) extends FilePipe(file) {
  val fileSize = 1024 * 1024 * 100L
  var fileChannel: FileChannel = null
  val charset: Charset = StandardCharsets.UTF_8
  val END_OF_MESSAGES: Int = -1 // 结束标记
  val INT_BYTES: Int = 4        // 4 字节用于存储长度
  var mappedBuffer: MappedByteBuffer = null

  override def create(): Unit = {
    if (file.exists()) {
      Runtime.getRuntime.exec(Array("rm", "-rf", file.getAbsolutePath))
    }
    val parentDir = file.getParentFile
    if (parentDir != null && parentDir.exists()) {
      val raf = new RandomAccessFile(file.getAbsolutePath, "rw")
      raf.setLength(fileSize)
      fileChannel = raf.getChannel
      mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
    }
  }


  override def dataFrame(): DataFrame =
    DefaultDataFrame(StructType.empty.add("content", StringType),
      ClosableIterator(read().map(str => Row.fromSeq(Seq(str))))())

  def write(messages: Iterator[String]): Unit = {
    if (mappedBuffer == null) throw new IllegalStateException("Not created")
    mappedBuffer.position(0)
    var finalPosition: Long = 0

    try {
      messages.foreach { message =>
        val messageBytes = message.getBytes(charset)
        if (mappedBuffer.remaining() < messageBytes.length + 1) {
          throw new IOException("Buffer overflow")
        }
        mappedBuffer.put(messageBytes)
        mappedBuffer.put('\n'.toByte)
      }

      // 1. 获取数据的真实结束位置
      finalPosition = mappedBuffer.position()

      // 2. [删除] 不再写入 '\0'
      // mappedBuffer.put(0.toByte)

      mappedBuffer.force()
    }

    // 3. [关键] 截断文件，移除所有 100MB 的填充
    // 这使得 pd.read_csv 可以工作
    try {
      fileChannel.truncate(finalPosition)
    } catch {
      case e: IOException =>
        println(s"Warning: Could not truncate file. ${e.getMessage}")
    }
  }

  def read(): ClosableIterator[String] = {
    if (mappedBuffer == null) throw new IllegalStateException("MMAPFilePipe not created. Call create() first.")

    // 重置缓冲区位置，准备从头读取
    mappedBuffer.position(0)

    val iter = new Iterator[String] {
      // 用于构建一行的临时字节缓冲区
      private val lineBuffer = new ByteArrayOutputStream(1024)
      private var nextLine: Option[String] = None
      private var eofReached = false

      // 预取下一行
      private def fetch(): Unit = {
        if (nextLine.isDefined || eofReached) {
          return // 已经有数据，或者已经结束
        }

        lineBuffer.reset() // 清空上一行的字节
        var continueReading = true

        while (continueReading && mappedBuffer.hasRemaining()) {
          val b = mappedBuffer.get() // 读取一个字节

          b match {
            case 0 => // 空字节 '\0'
              // 遇到空字节，这是Python写入的数据的末尾
              continueReading = false
              eofReached = true

            case '\n' => // 换行符
              // 这一行结束了
              continueReading = false

            case '\r' =>
            // 忽略回车符, 兼容 \r\n

            case _ =>
              // 普通字节
              lineBuffer.write(b)
          }
        }

        if (!mappedBuffer.hasRemaining()) {
          // 到达了 MappedBuffer 的物理末尾
          eofReached = true
        }

        if (lineBuffer.size() > 0) {
          // 如果缓冲区有数据 (即使是在EOF之后)，也将其作为最后一行
          nextLine = Some(new String(lineBuffer.toByteArray, charset))
        } else if (!eofReached) {
          // 缓冲区为空，但我们遇到了'\n' (一个空行)
          nextLine = Some("")
        } else {
          // 缓冲区为空，并且已达文件末尾
          nextLine = None
        }
      }

      override def hasNext: Boolean = {
        fetch() // 确保已尝试获取
        nextLine.isDefined
      }

      override def next(): String = {
        if (!hasNext) throw new NoSuchElementException("No more lines")
        val line = nextLine.get
        nextLine = None // 消费掉这一行
        line
      }
    }
    ClosableIterator(iter)(() => {})
  }

}

object MMAPFilePipe {

  def fromFilePath(path: String): MMAPFilePipe = {
    val pipe = new MMAPFilePipe(new File(path))
    pipe.create()
    pipe
  }

  def fromFile(file: File): MMAPFilePipe = {
    val pipe = new MMAPFilePipe(file)
    pipe.create()
    pipe
  }

}