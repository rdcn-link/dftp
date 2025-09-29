package link.rdcn.struct

import java.io.InputStream
import java.util.UUID
import scala.collection.concurrent.TrieMap

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/26 10:56
 * @Modified By:
 */
object BlobRegistry {

  private val handlers = TrieMap[String, Blob]()

  def register(blob: Blob): String = {
    val blobId = UUID.randomUUID().toString
    handlers.put(blobId, blob)
    blobId
  }

  def getBlob(id: String): Option[Blob] = handlers.get(id)

  def getStream[T](id: String)(consume: InputStream => T): Option[T] = {
    handlers.get(id).map { blob =>
      blob.offerStream(consume)
    }
  }

  def cleanUp(): Unit = handlers.clear()
}