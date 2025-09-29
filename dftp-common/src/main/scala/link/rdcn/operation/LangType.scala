package link.rdcn.operation

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 09:22
 * @Modified By:
 */

trait LangType {
  def name: String
}

object LangType {
  case object JAVA_BIN extends LangType {
    val name = "JAVA_BIN"
  }

  case object PYTHON_CODE extends LangType {
    val name = "PYTHON_CODE"
  }
}

