package link.rdcn.dacp.optree

import link.rdcn.operation.LangType

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/23 17:23
 * @Modified By:
 */
object LangTypeV2 {

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

  case object CPP_BIN extends LangType {
    val name = "CPP_BIN"
  }

  case object REPOSITORY_OPERATOR extends LangType {
    val name = "REPOSITORY_OPERATOR"
  }

  case object FILE_REPOSITORY_BUNDLE extends LangType {
    val name = "FileRepositoryBundle"
  }

}
