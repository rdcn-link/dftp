package link.rdcn.struct

import link.rdcn.util.DataUtils

import scala.annotation.varargs

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/23 16:23
 * @Modified By:
 */
trait DataFrame {
  val schema: StructType

  val dataFrameShape: DataFrameShape

  def mapIterator[T](f: ClosableIterator[Row] => T): T

  def map(f: Row => Row): DataFrame

  def filter(f: Row => Boolean): DataFrame

  @varargs
  def select(columns: String*): DataFrame

  def limit(n: Int): DataFrame

  def foreach(f: Row => Unit): Unit

  def collect(): List[Row]

  def getDataFrameStatistic: DataFrameStatistics = DataFrameStatistics.empty()
}

object DataFrame {

  def create(dataStreamSource: DataStreamSource): DataFrame = {
    DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
  }

  def fromSeq(seq: Seq[Any]): DataFrame = {
    val stream = ClosableIterator(seq.map(value => Row.fromSeq(Seq(value))).toIterator)(() => {})
    DataUtils.getDataFrameByStream(stream)
  }

  def fromMap(maps: Seq[Map[String, Any]]): DataFrame = {
    val stream = ClosableIterator(maps.map(m => Row.fromSeq(m.values.toSeq)).toIterator)(() => {})
    DefaultDataFrame(DataUtils.getStructTypeFromMap(maps.head), stream)
  }

  def empty(): DataFrame = {
    DefaultDataFrame(StructType.empty, ClosableIterator(Iterator.empty)())
  }
}