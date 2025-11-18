package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.struct.ValueType.StringType

import java.io.File

object FileType extends Enumeration {
  type FileType = Value
  val FIFO_BUFFER, RAM_FILE, MMAP_FILE, DIRECTORY = Value

  override def toString: String = this match {
    case FIFO_BUFFER => "FIFO_BUFFER"
    case RAM_FILE => "RAM_FILE"
    case MMAP_FILE => "MMAP_FILE"
    case DIRECTORY => "DIRECTORY"
    case _ => throw new NoSuchElementException("Unknown FileType")
  }

  // Parses a string into a FileType
  def fromString(str: String): FileType = str.toUpperCase match {
    case "FIFO_BUFFER" => FIFO_BUFFER
    case "RAM_FILE" => RAM_FILE
    case "MMAP_FILE" => MMAP_FILE
    case "DIRECTORY" => DIRECTORY
    case _ => throw new NoSuchElementException(s"No value found for '$str'")
  }
}

//TODO
case class FileDirectoryDataFrame(files: Seq[(File, FileType.FileType)])

case class FileDataFrame(file: File, fileType: FileType.FileType) extends DataFrame{

  lazy val df = RowFilePipe(file).dataFrame()

  override val schema: StructType = StructType.empty.add("content", StringType)

  override def mapIterator[T](f: ClosableIterator[Row] => T): T = df.mapIterator(f)

  override def map(f: Row => Row): DataFrame = df.map(f)

  override def filter(f: Row => Boolean): DataFrame = df.filter(f)

  override def select(columns: String*): DataFrame = df.select(columns: _*)

  override def limit(n: Int): DataFrame = df.limit(n)

  override def foreach(f: Row => Unit): Unit = df.foreach(f)

  override def collect(): List[Row] = df.collect()
}
