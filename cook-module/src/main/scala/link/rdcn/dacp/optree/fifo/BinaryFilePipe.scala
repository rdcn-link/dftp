package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType}

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.ByteBuffer
import java.io._
/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/29 14:24
 * @Modified By:
 */

case class BinaryFilePipe(file: File) extends FilePipe (file){

  private val DefaultChunkSize = 8192  // 8KB 默认块大小

  def write(dataIterator: Iterator[Array[Byte]], chunkSize: Int = DefaultChunkSize): Unit = {
    val path = Paths.get(file.getAbsolutePath)
    val channel = Files.newByteChannel(
      path,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )

    try {
      dataIterator.foreach { chunk =>
        val buffer = ByteBuffer.wrap(chunk)
        while (buffer.hasRemaining) {
          channel.write(buffer)
        }
      }
    } finally {
      channel.close()
    }
  }

  def read(chunkSize: Int = DefaultChunkSize): Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
    private val inputStream = new BufferedInputStream(new FileInputStream(file), chunkSize)
    private val buffer = new Array[Byte](chunkSize)
    private var bytesRead: Int = -1
    private var isClosed = false

    bytesRead = inputStream.read(buffer)

    override def hasNext: Boolean = {
      if (bytesRead == -1 && !isClosed) {
        close()
        false
      } else {
        bytesRead != -1
      }
    }

    override def next(): Array[Byte] = {
      if (!hasNext) throw new NoSuchElementException("No more data")
      val result = buffer.take(bytesRead)

      bytesRead = inputStream.read(buffer)
      if (bytesRead == -1) {
        close()
      }

      result
    }

    private def close(): Unit = {
      if (!isClosed) {
        inputStream.close()
        isClosed = true
      }
    }
  }

  def fromExistFile(sourceFile: File): BinaryFilePipe = {
    val bytes = Files.readAllBytes(Paths.get(sourceFile.getPath))
    new Thread(() => {
      write(Seq(bytes).iterator)
    }).start()
    this
  }

  override def dataFrame(): DataFrame =
    DefaultDataFrame(StructType.binaryStructType, read().map(bytes => Row.fromSeq(Seq(bytes))))

  override def write(messages: Iterator[String]): Unit = ???

  override def read(): ClosableIterator[String] = ???
}

object BinaryFilePipe {

  def createEmptyFile(path: String): BinaryFilePipe = {
    val pipe = new BinaryFilePipe(new File(path))
    pipe.create()
    pipe
  }

  def createEmptyFile(file: File): BinaryFilePipe = {
    val pipe = new BinaryFilePipe(file)
    pipe.create()
    pipe
  }

}
