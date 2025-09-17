package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 17:20
 * @Modified By:
 */
case class ClosableIterator[T](
                                underlying: Iterator[T],
                                val onClose: () => Unit,
                                val isFileList: Boolean = false
                              ) extends Iterator[T] with AutoCloseable {

  private var closed = false
  //避免多次触发close
  private var hasMore = true

  override def hasNext: Boolean = {
    if (!hasMore || closed) return false
    val more = underlying.hasNext
    if (!more && !isFileList) {
      hasMore = false
      close()
    }
    more
  }

  override def next(): T = {
    if (!hasNext && !isFileList) throw new NoSuchElementException("next on empty iterator")
    val value = underlying.next()
    if (!underlying.hasNext && !isFileList) {
      hasMore = false
      close()
    }
    value
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      try onClose()
      catch {
        case ex: Throwable => throw new Exception(s"[ClosableIterator] Error during close: ${ex.getMessage}")
      }
    }
  }
}

object ClosableIterator {
  def apply[T](underlying: Iterator[T])(onClose: => Unit): ClosableIterator[T] =
    new ClosableIterator[T](underlying, () => onClose)
}
