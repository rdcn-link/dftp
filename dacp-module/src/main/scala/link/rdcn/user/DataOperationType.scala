package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/8 18:14
 * @Modified By:
 */

sealed trait DataOperationType

object DataOperationType {

  case object Map extends DataOperationType
  case object Filter extends DataOperationType
  case object Select extends DataOperationType
  case object Reduce extends DataOperationType
  case object Join extends DataOperationType
  case object GroupBy extends DataOperationType
  case object Sort extends DataOperationType

  val values: Seq[DataOperationType] = Seq(
    Map, Filter, Select, Reduce, Join, GroupBy, Sort
  )

  def fromString(name: String): Option[DataOperationType] =
    values.find(_.toString.equalsIgnoreCase(name))
}