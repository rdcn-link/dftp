package link.rdcn.server

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/19 16:54
 * @Modified By:
 */
trait DftpTicket{
  val typeId: Byte
  val ticketContent: String

  def encodeTicket(): Array[Byte] = {
    val b = ticketContent.getBytes(StandardCharsets.UTF_8)
    val buffer = java.nio.ByteBuffer.allocate(1 + 4 + b.length)
    buffer.put(typeId)
    buffer.putInt(b.length)
    buffer.put(b)
    buffer.array()
  }
}

object DftpTicket{
  val BLOB_TICKET: Byte = 1
  val Get_TICKET: Byte = 2

  def decodeTicket(bytes: Array[Byte]): DftpTicket = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val typeId: Byte = buffer.get()
    val len = buffer.getInt()
    val b = new Array[Byte](len)
    buffer.get(b)
    val ticketContent = new String(b, StandardCharsets.UTF_8)
    typeId match {
      case BLOB_TICKET => BlobTicket(ticketContent)
      case Get_TICKET => GetTicket(ticketContent)
    }
  }
}

case class BlobTicket(ticketContent: String) extends DftpTicket {
  override val typeId: Byte = DftpTicket.BLOB_TICKET
}

case class GetTicket(ticketContent: String) extends DftpTicket {
  override val typeId: Byte = DftpTicket.Get_TICKET
}


