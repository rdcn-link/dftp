package link.rdcn.message

import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.Ticket

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/19 16:54
 * @Modified By:
 */
//trait DftpTicket
//{
//  val typeId: Byte
//  val ticketContent: String
//
//  def encodeTicket(): Array[Byte] =
//  {
//    val b = ticketContent.getBytes(StandardCharsets.UTF_8)
//    val buffer = java.nio.ByteBuffer.allocate(1 + 4 + b.length)
//    buffer.put(typeId)
//    buffer.putInt(b.length)
//    buffer.put(b)
//    buffer.array()
//  }
//}
//
//case class BlobTicket(blobId: String) extends DftpTicket
//{
//  override val typeId: Byte = 1
//  override val ticketContent: String = blobId
//}
//
//case class GetTicket(url: String) extends DftpTicket
//{
//  override val typeId: Byte = 2
//  override val ticketContent: String = url
//}

object DftpTicket{

  def createTicket(ticketId: String): Ticket =
    new Ticket(CodecUtils.encodeString(ticketId))

  def getTicketId(ticket: Ticket): String =
    CodecUtils.decodeString(ticket.getBytes)

}

trait ActionMethodType{
  def name: String
}

object ActionMethodType {

  case object GetTabularMeta extends ActionMethodType {
    override def name: String = "GET_TABULAR_META"
  }

  private var extraTypes: Map[String, ActionMethodType] = Map.empty

  def registerExtraType(t: ActionMethodType): Unit = {
    extraTypes += t.name -> t
  }

  def fromString(name: String): ActionMethodType = {
    name match {
      case GetTabularMeta.name => GetTabularMeta
      case other     => extraTypes.getOrElse(name,
        throw new IllegalArgumentException(s"Unknown ActionMethodType: $other"))
    }
  }

}


