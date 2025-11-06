package link.rdcn.dacp.catalog

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/3 14:41
 * @Modified By:
 */
trait CatalogServiceRequest {
  def getDataSetId: String

  def getDataFrameUrl: String
}
