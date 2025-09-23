package link.rdcn.struct

import link.rdcn.struct.ValueType.RefType

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 14:35
 * @Modified By:
 */

sealed trait ValueType {
  def name: String

  override def toString: String = name
}

trait DFValue {
  def value: Any

  def valueType: ValueType
}

case class DFRef(url: String) extends DFValue {

  override def value: Any = url

  override def valueType: ValueType = RefType

}


object ValueType {
  case object IntType extends ValueType {
    val name = "Int"
  }

  case object LongType extends ValueType {
    val name = "Long"
  }

  case object FloatType extends ValueType {
    val name = "Float"
  }

  case object DoubleType extends ValueType {
    val name = "Double"
  }

  case object StringType extends ValueType {
    val name = "String"
  }

  case object BooleanType extends ValueType {
    val name = "Boolean"
  }

  case object BinaryType extends ValueType {
    val name = "Binary"
  } // 字节数组

  case object RefType extends ValueType {
    val name = "REF"
  }

  case object BlobType extends ValueType {
    val name = "Blob"
  }

  case object NullType extends ValueType {
    val name = "Null"
  }

  val values: Seq[ValueType] = Seq(
    IntType, LongType, FloatType, DoubleType,
    StringType, BooleanType, BinaryType,
    NullType
  )

  /**
   * 从字符串恢复类型（不区分大小写）。
   * 支持常见同义词：如 "int"/"integer"，"bytes"/"binary" 等。
   */
  def fromName(name: String): Option[ValueType] = {
    val lower = name.trim.toLowerCase
    lower match {
      case "int" | "integer" => Some(IntType)
      case "long" => Some(LongType)
      case "float" => Some(FloatType)
      case "double" => Some(DoubleType)
      case "string" => Some(StringType)
      case "boolean" | "bool" => Some(BooleanType)
      case "binary" | "bytes" => Some(BinaryType)
      case "null" => Some(NullType)
      case _ => throw new Exception(s"The data type does not exist $lower")
    }
  }

  /** 是否是数值类型 */
  def isNumeric(t: ValueType): Boolean =
    t match {
      case IntType | LongType | FloatType | DoubleType => true
      case _ => false
    }
}

object ValueTypeHelper {

  def getAllTypes: Seq[ValueType] = ValueType.values

  /** 获取某个具体类型（便于 Java 调用） */
  def getIntType: ValueType = ValueType.IntType

  def getLongType: ValueType = ValueType.LongType

  def getFloatType: ValueType = ValueType.FloatType

  def getDoubleType: ValueType = ValueType.DoubleType

  def getStringType: ValueType = ValueType.StringType

  def getBooleanType: ValueType = ValueType.BooleanType

  def getBinaryType: ValueType = ValueType.BinaryType

  def getNullType: ValueType = ValueType.NullType

  /** 是否是数值类型 */
  def isNumeric(t: ValueType): Boolean =
    ValueType.isNumeric(t)

  /** 从字符串解析 ValueType */
  def fromName(name: String): ValueType =
    ValueType.fromName(name).getOrElse(
      throw new IllegalArgumentException(s"Invalid type: $name")
    )
}


