package link.rdcn.struct

import org.json.JSONObject

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}
import scala.reflect.ClassTag

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 15:05
 * @Modified By:
 */

class Row(val values: Seq[Any]) {
  /** 行长度（列数） */
  def length: Int = values.length

  /** 按索引获取值，越界则抛 IndexOutOfBoundsException */
  def get(index: Int): Any = values(index)

  /** 按索引获取 Option，如果越界返回 None */
  def getOpt(index: Int): Option[Any] =
    if (index >= 0 && index < values.length) Some(values(index)) else None

  /** 将行转为 Seq[Any] */
  def toSeq: Seq[Any] = values

  /** 转成 Array[Any] */
  def toArray: Array[Any] = values.toArray

  /** 获取迭代器 */
  def iterator: Iterator[Any] = values.iterator

  def isEmpty: Boolean = values.isEmpty

  def toJsonString(structType: StructType): String = {
    val jo = new JSONObject()
    structType.columns.map(_.name).zip(values).foreach(kv => jo.put(kv._1, kv._2))
    jo.toString
  }

  def toJsonObject(structType: StructType): JSONObject = {
    val jo = new JSONObject()
    structType.columns.map(_.name).zip(values).foreach(kv => jo.put(kv._1, kv._2))
    jo
  }

  /** 在开头插入一个值，返回新的 Row */
  def prepend(value: Any): Row = new Row(value +: values)

  /** 在末尾追加一个值，返回新的 Row */
  def append(value: Any): Row = new Row(values :+ value)

  override def toString: String = {
    val elems = values.map {
      case null => ("null")
      case arr: Array[_] => arr.mkString("Array(", ", ", ")")
      case other => other.toString
    }
    s"Row(${elems.mkString(", ")})"
  }

  /** getAs[T]: 强转为 T，失败或值为 null 则抛出异常 */
  def getAs[T: ClassTag](index: Int): T = getOpt(index) match {
    case Some(v) =>
      if (v == null) {
        null.asInstanceOf[T]
      } else {
        try v.asInstanceOf[T]
        catch {
          case e: ClassCastException =>
            throw new IllegalArgumentException(
              s"Value at index $index cannot be cast to the expected type ${implicitly[ClassTag[T]]}", e)
        }
      }
    case None =>
      throw new NoSuchElementException(s"No value found at index $index")
  }

  def _1: Any = get(0)

  def _2: Any = get(1)

  def _3: Any = get(2)

  def _4: Any = get(3)

  def _5: Any = get(4)

  def _6: Any = get(5)

  def _7: Any = get(6)

  def _8: Any = get(7)

  def _9: Any = get(8)

  def _10: Any = get(9)

  def _11: Any = get(10)

  def _12: Any = get(11)

  def _13: Any = get(12)

  def _14: Any = get(13)

  def _15: Any = get(14)

  def _16: Any = get(15)

  def _17: Any = get(16)

  def _18: Any = get(17)

  def _19: Any = get(18)

  def _20: Any = get(19)

  def _21: Any = get(20)

  def _22: Any = get(21)
}

object Row {
  /** Varargs 方式构造：Row(1, "Li", true) 等 */
  def apply(values: Any*): Row = new Row(values)

  /** 从 Seq[Any] 构造 */
  def fromSeq(seq: Seq[Any]): Row = new Row(seq)

  /** 从 Array[Any] 构造 */
  def fromArray(arr: Array[Any]): Row = new Row(arr.toSeq)

  def fromTuple(tuple: Product): Row = fromSeq(tuple.productIterator.toSeq)

  def fromJavaList(list: java.util.List[Object]): Row = {
    new Row(list.asScala)
  }

  def fromJsonString(jsonStr: String): Row = {
    val jo = new JSONObject(jsonStr)
    Row.fromSeq(jo.keys().asScala.toSeq.map(jo.get(_)))
  }

  /** 空行 */
  val empty: Row = new Row(Seq.empty)

  /** Helper: 与索引和值一起迭代 */
  def zipWithIndex(row: Row): Seq[(Int, Any)] =
    row.values.zipWithIndex.map { case (v, idx) => (idx, v) }
}

