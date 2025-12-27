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
case class DftpTicket(ticketId: String) {
  val ticket: Ticket = new Ticket(CodecUtils.encodeString(ticketId))
}

object DftpTicket{
  def getDftpTicket(ticket: Ticket): DftpTicket =
    DftpTicket(CodecUtils.decodeString(ticket.getBytes))
}

trait ActionMethodType{
  def name: String
}

object ActionMethodType {

  case object GetTabular extends ActionMethodType {
    override def name: String = "GET_TABULAR"
  }

  case object GetBlob extends ActionMethodType {
    override def name: String = "GET_BLOB"
  }

  private var extraTypes: Map[String, ActionMethodType] = Map.empty

  def registerExtraType(t: ActionMethodType): Unit = {
    extraTypes += t.name -> t
  }

  def fromString(name: String): ActionMethodType = {
    name match {
      case GetTabular.name => GetTabular
      case other     => extraTypes.getOrElse(name,
        throw new IllegalArgumentException(s"Unknown ActionMethodType: $other"))
    }
  }

}


