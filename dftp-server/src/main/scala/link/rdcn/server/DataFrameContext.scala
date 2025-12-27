package link.rdcn.server

import link.rdcn.message.DftpTicket
import link.rdcn.struct.{Blob, DataFrame, DataFrameMetaData}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/27 13:11
 * @Modified By:
 */

trait DataFrameContext {

  def getDataFrameMeta: DataFrameMetaData

  def getDataFrame: DataFrame

  final def getDftpTicket: DftpTicket = DftpTicket(ServedDataFramePool.registry(getDataFrame))

}

trait BlobContext {

  def getBlob: Blob

  final def getDftpTicket: DftpTicket = DftpTicket(ServedDataFramePool.registry(getBlob))

}
