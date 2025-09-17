package link.rdcn.operation

import link.rdcn.struct.{DataFrame, Row}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:42
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait GenericFunctionCall extends Serializable {
  def transform(input: Any): Any
}

case class SingleRowCall(f: SerializableFunction[Row, Any]) extends GenericFunctionCall {
  override def transform(input: Any): Any = input match {
    case row: Row => f(row)
    case _ => throw new IllegalArgumentException(s"Expected Row but got ${input.getClass}")
  }
}

case class RowPairCall(f: SerializableFunction[(Row, Row), Any]) extends GenericFunctionCall {
  override def transform(input: Any): Any = input match {
    case (r1: Row, r2: Row) => f((r1, r2))
    case _ => throw new IllegalArgumentException(s"Expected (Row, Row) but got ${input}")
  }
}

case class IteratorRowCall(f: SerializableFunction[Iterator[Row], Any]) extends GenericFunctionCall {
  override def transform(input: Any): Any = input match {
    case r: Iterator[Row] => f(r)
    case _ => throw new IllegalArgumentException(s"Expected Iterator[Row] but got ${input}")
  }
}

case class DataFrameCall11(f: SerializableFunction[DataFrame, DataFrame]) extends GenericFunctionCall {
  override def transform(input: Any): Any = input match {
    case r: DataFrame => f(r)
    case _ => throw new IllegalArgumentException(s"Expected DataFrame but got ${input}")
  }
}

case class DataFrameCall21(f: SerializableFunction[(DataFrame, DataFrame), DataFrame]) extends GenericFunctionCall {
  override def transform(input: Any): Any = input match {
    case r: (DataFrame, DataFrame) => f(r)
    case _ => throw new IllegalArgumentException(s"Expected (DataFrame,DataFrame) but got ${input}")
  }
}


