package link.rdcn.dacp.optree.fifo

import link.rdcn.struct.{ClosableIterator, DataFrame}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.io.File
import java.nio.file.Files
import scala.concurrent.{Await, Future}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/29 14:57
 * @Modified By:
 */
abstract class FilePipe(file: File) {
  def create(): Unit = {
    if (file.exists()) {
      Runtime.getRuntime.exec(Array("rm", "-rf", file.getAbsolutePath))
    }
    Runtime.getRuntime.exec(Array("mkfifo", file.getAbsolutePath)).waitFor()
  }

  def write(messages: Iterator[String]): Unit

  def read(): ClosableIterator[String]

  def copyToFile(otherPipe: FilePipe): Unit = {
    println(s"${this.file.getAbsolutePath} copying to ${otherPipe.path}")
    otherPipe match {
      case s if s.isInstanceOf[RowFilePipe] => Future {
        otherPipe.write(read())
      }
      case others =>
        Await.result(Future {
          otherPipe.write(read())
        },1.minute)

    }

  }

  def delete(): Unit = {
    if(file.exists()) {
      Files.deleteIfExists(file.toPath)
    }
  }

  def path: String = file.getAbsolutePath

  def dataFrame(): DataFrame
}

object FilePipe {
  def fromFilePath(path: String, fileType: FileType.FileType): FilePipe = {
    fileType match {
      case t if t == FileType.FIFO_BUFFER => RowFilePipe.fromFilePath(path)
      case t if t == FileType.RAM_FILE => RAMFilePipe.fromFilePath(path)
      case t if t == FileType.MMAP_FILE => MMAPFilePipe.fromFilePath(path)
    }
  }

  def getFilePipe(path: String, fileType: FileType.FileType): FilePipe = {
    fileType match {
      case t if t == FileType.FIFO_BUFFER => RowFilePipe(new File(path))
      case t if t == FileType.RAM_FILE => RAMFilePipe(new File(path))
      case t if t == FileType.MMAP_FILE => MMAPFilePipe(new File(path))
    }
  }
}

