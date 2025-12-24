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

/**
 * DftpTicket wraps an Arrow Flight Ticket with a JSON string payload.
 *
 * @param jsonStr JSON string representing the ticket content
 */
final case class DftpTicket(ticketId: String) {
  val ticket: Ticket = new Ticket(CodecUtils.encodeString(ticketId))
}

trait GetStreamType {
  def name: String
}

object GetStreamType {

  case object Blob extends GetStreamType {
    override val name: String = "blob"
  }

  case object Get extends GetStreamType {
    override val name: String = "get"
  }

  /** Optional: for future extensibility, modules can register new types */
  private var extraTypes: Map[String, GetStreamType] = Map.empty

  def registerExtraType(t: GetStreamType): Unit = {
    extraTypes += t.name.toLowerCase -> t
  }

  def fromString(name: String): GetStreamType = {
    name.toLowerCase match {
      case Blob.name => Blob
      case Get.name  => Get
      case other     => extraTypes.getOrElse(name.toLowerCase,
        throw new IllegalArgumentException(s"Unknown GetStreamType: $other"))
    }
  }
}


