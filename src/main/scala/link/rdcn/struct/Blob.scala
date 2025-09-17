package link.rdcn.struct

import java.io.{File, FileInputStream, InputStream}

trait Blob extends FairdValue {

  def offerStream[T](consume: InputStream => T): T

  override def value: Any = this

  override def valueType: ValueType = ValueType.BlobType

  override def toString: String = s"Blob Value"
}

object Blob {
  def fromFile(file: File): Blob = {
    new Blob {
      override def offerStream[T](consume: java.io.InputStream => T): T = {
        var stream: InputStream = null
        try {
          stream = new FileInputStream(file)
          consume(stream)
        } finally {
          if (stream != null) stream.close()
        }
      }
    }
  }
}