package link.rdcn.dacp.optree.fifo

import link.rdcn.dacp.optree.TransformerNode
import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.util.DataUtils
import link.rdcn.struct.ValueType.StringType

import java.io.{BufferedReader, File, FileReader, FileWriter, PrintWriter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 14:21
 * @Modified By:
 */
case class RowFilePipe(file: File) extends FilePipe(file) {
  def write(messages: Iterator[String]): Unit = {
    val writer = new PrintWriter(new FileWriter(file))
    try {
      messages.foreach { message =>
        writer.println(message)
        writer.flush()
      }
    } finally {
      writer.close()
    }
  }

  def read(): ClosableIterator[String] = {
    val iter = new Iterator[String] {
      private val reader = new BufferedReader(new FileReader(file))
      private var nextLine: String = reader.readLine()
      private var isClosed = false

      override def hasNext: Boolean = {
        if (nextLine == null && !isClosed) {
          reader.close()
          isClosed = true
          false
        } else {
          nextLine != null
        }
      }

      override def next(): String = {
        if (!hasNext) throw new NoSuchElementException("No more lines")
        val current = nextLine
        nextLine = reader.readLine()
        if (nextLine == null) {
          reader.close()
          isClosed = true
        }
        current
      }
    }
    ClosableIterator(iter)(() => {})
  }

  def fromExistFile(sourceFile: File): RowFilePipe = {
    write(DataUtils.getFileLines(sourceFile))
    this
  }

  override def dataFrame(): DataFrame =
    DefaultDataFrame(StructType.empty.add("content", StringType),
      ClosableIterator(read().map(str => Row.fromSeq(Seq(str))))())
}

object RowFilePipe {

  def fromFilePath(path: String): RowFilePipe = {
    val pipe = new RowFilePipe(new File(path))
    pipe.create()
    pipe
  }

  def fromFile(file: File): RowFilePipe = {
    val pipe = new RowFilePipe(file)
    pipe.create()
    pipe
  }

}