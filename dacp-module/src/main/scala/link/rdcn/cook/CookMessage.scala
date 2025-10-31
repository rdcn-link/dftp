package link.rdcn.cook

import link.rdcn.message.DftpTicket
import link.rdcn.operation.TransformOp
import link.rdcn.server.DftpGetStreamRequest

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/20 15:24
 * @Modified By:
 */
case class CookTicket(ticketContent: String) extends DftpTicket {
  override val typeId: Byte = 3
}

trait DacpCookStreamRequest extends DftpGetStreamRequest{
  def getTransformTree: TransformOp
}
