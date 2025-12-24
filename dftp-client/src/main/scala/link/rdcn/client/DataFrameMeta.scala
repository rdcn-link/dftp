package link.rdcn.client

import link.rdcn.message.DftpTicket
import link.rdcn.struct.{DataFrameShape, StructType}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/24 15:43
 * @Modified By:
 */
trait DataFrameMeta {

  def getDataFrameShape: DataFrameShape

  def getDataFrameSchema: StructType

  def getStreamTicket: DftpTicket

}
