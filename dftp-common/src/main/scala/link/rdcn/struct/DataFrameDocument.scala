package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/10 16:21
 * @Modified By:
 */
trait DataFrameDocument extends Serializable {
  def getSchemaURL(): Option[String]

  def getDataFrameTitle(): Option[String]

  def getColumnURL(colName: String): Option[String]

  def getColumnAlias(colName: String): Option[String]

  def getColumnTitle(colName: String): Option[String]
}

object DataFrameDocument{

  def empty(): DataFrameDocument = {
    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = None

      override def getDataFrameTitle(): Option[String] = None

      override def getColumnURL(colName: String): Option[String] = None

      override def getColumnAlias(colName: String): Option[String] = None

      override def getColumnTitle(colName: String): Option[String] = None
    }
  }

}
