package link.rdcn.operation

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 09:22
 * @Modified By:
 */

sealed trait LangType {
  def name: String
}

object LangType {
  case object JAVA_BIN extends LangType {
    val name = "JAVA_BIN"
  }

  case object JAVA_CODE extends LangType {
    val name = "JAVA_CODE"
  }

  case object JAVA_JAR extends LangType {
    val name = "JAVA_JAR"
  }

  case object PYTHON_CODE extends LangType {
    val name = "PYTHON_CODE"
  }

  case object PYTHON_BIN extends LangType {
    val name = "PYTHON_BIN"
  }

  case object TYPESCRIPT_CODE extends LangType {
    val name = "TYPESCRIPT_CODE"
  }

  case object CPP_BIN extends LangType {
    val name = "CPP_BIN"
  }

  case object REPOSITORY_OPERATOR extends LangType {
    val name = "REPOSITORY_OPERATOR"
  }

  val all: List[LangType] = List(JAVA_BIN, PYTHON_CODE, TYPESCRIPT_CODE)

  def fromName(s: String): LangType = all.find(_.name == s)
    .getOrElse(throw new Exception(s"$s This programming language is not supported"))
}

