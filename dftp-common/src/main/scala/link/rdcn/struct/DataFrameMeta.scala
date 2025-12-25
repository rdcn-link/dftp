package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/24 15:43
 * @Modified By:
 */
trait DataFrameMeta {

  def getDataFrameShape: DataFrameShape

  def getDataFrameSchema: StructType

  def getDataFrameDocument: DataFrameDocument = DataFrameDocument.empty()

  def getDataFrameStatistic: DataFrameStatistics = DataFrameStatistics.empty()

}
