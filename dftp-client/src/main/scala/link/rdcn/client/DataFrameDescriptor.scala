package link.rdcn.client

import link.rdcn.struct.DataFrameMeta
import org.apache.arrow.flight.Ticket

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/25 17:08
 * @Modified By:
 */
trait DataFrameDescriptor {

  def getDataFrameMeta: DataFrameMeta

  def getDataFrameTicket: Ticket

}
