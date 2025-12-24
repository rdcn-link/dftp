package link.rdcn.struct

import org.json.JSONObject

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/24 15:06
 * @Modified By:
 */
sealed trait DataFrameShape {
  def name: String
  def dimensions: Int
  def description: String
}

object DataFrameShape {

  case object BlobShape extends DataFrameShape {
    override val name: String = "blobShape"
    override val dimensions: Int = 0
    override val description: String = "0-D Blob / Binary Object"
  }

  case object Tabular extends DataFrameShape {
    override val name: String = "tabular"
    override val dimensions: Int = 2
    override val description: String = "2-D Tabular DataFrame"
  }

  final case class ND(dims: Int) extends DataFrameShape {
    require(dims > 2, "ND frame must have dimensions > 2")

    override val name: String = s"nd:$dims"
    override val dimensions: Int = dims
    override val description: String = s"$dims-D Multidimensional Frame"
  }

  def fromName(name: String): DataFrameShape = {
    name.toLowerCase match {
      case "blobShape" =>
        BlobShape

      case "tabular" =>
        Tabular

      case nd if nd.startsWith("nd:") =>
        val dims = nd.stripPrefix("nd:").toInt
        ND(dims)

      case other =>
        throw new IllegalArgumentException(s"Unknown DataFrameShape name: $other")
    }
  }
}





