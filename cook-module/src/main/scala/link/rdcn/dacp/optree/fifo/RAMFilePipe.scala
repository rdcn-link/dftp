package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._

import java.io._
import java.nio.file.{Files, Paths}

case class RAMFilePipe(file: File) extends FilePipe(file) {
  val prefix = "/dev/shm"
  val target = Paths.get(prefix,file.getName).toString
  val targetFile = new File(target)

  override def create(): Unit = {
    if (file.exists()) {
      println(s"删除RAM_FILE ${file.getAbsolutePath} 中...")
      Files.deleteIfExists(file.toPath)
    }

    Runtime.getRuntime.exec(Array("touch", target)).waitFor()
    Runtime.getRuntime.exec(Array("ln", "-sf", target, file.getAbsolutePath)).waitFor()

  }

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

  override def delete(): Unit = {
    if (file.exists()) {
      println(s"删除RAM_FILE ${file.getAbsolutePath} 中...")
      Files.deleteIfExists(file.toPath)
    }
    if (targetFile.exists() && !Files.isDirectory(targetFile.toPath)) {
      println(s"删除RAM_FILE ${target} 中...")
      Files.deleteIfExists(targetFile.toPath)
    }
  }



  override def dataFrame(): DataFrame =
    DefaultDataFrame(StructType.empty.add("content", StringType),
      ClosableIterator(read().map(str => Row.fromSeq(Seq(str))))())
}

object RAMFilePipe {

  def fromFilePath(path: String): RAMFilePipe = {
    val pipe = new RAMFilePipe(new File(path))
    pipe.create()
    pipe
  }

  def fromFile(file: File): RAMFilePipe = {
    val pipe = new RAMFilePipe(file)
    pipe.create()
    pipe
  }

}