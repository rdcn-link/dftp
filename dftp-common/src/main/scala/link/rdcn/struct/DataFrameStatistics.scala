package link.rdcn.struct

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 13:49
 * @Modified By:
 */
trait DataFrameStatistics extends Serializable {
  def rowCount: Long

  def byteSize: Long
}

object DataFrameStatistics {

  def empty(): DataFrameStatistics =
    new DataFrameStatistics {
      override def rowCount: Long = -1L

      override def byteSize: Long = -1L
    }

}
