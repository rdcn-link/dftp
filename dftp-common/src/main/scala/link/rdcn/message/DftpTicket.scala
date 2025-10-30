package link.rdcn.message

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/19 16:54
 * @Modified By:
 */
trait DftpTicket
{
  val typeId: Byte
  val ticketContent: String

  def encodeTicket(): Array[Byte] =
  {
    val b = ticketContent.getBytes(StandardCharsets.UTF_8)
    val buffer = java.nio.ByteBuffer.allocate(1 + 4 + b.length)
    buffer.put(typeId)
    buffer.putInt(b.length)
    buffer.put(b)
    buffer.array()
  }
}

case class BlobTicket(blobId: String) extends DftpTicket
{
  override val typeId: Byte = 1
  override val ticketContent: String = blobId
}

case class GetTicket(url: String) extends DftpTicket
{
  override val typeId: Byte = 2
  override val ticketContent: String = url
}


