package link.rdcn.struct

import link.rdcn.util.{DataUtils, ResourceUtils}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 15:51
 * @Modified By:
 */

case class DefaultDataFrame(
                             schema: StructType,
                             stream: ClosableIterator[Row],
                             dataFrameStatistics: DataFrameStatistics = DataFrameStatistics.empty()
                           ) extends DataFrame {

  override def map(f: Row => Row): DataFrame = {
    val iter = ClosableIterator(stream.map(f(_)))(stream.close())
    DataUtils.getDataFrameByStream(iter)
  }

  override def filter(f: Row => Boolean): DataFrame = {
    val iter = ClosableIterator(stream.filter(f(_)))(stream.close())
    DataUtils.getDataFrameByStream(iter)
  }

  override def select(columns: String*): DataFrame = {
    val selectedSchema = schema.select(columns: _*)
    val selectedStream = stream.map { row =>
      val selectedValues = columns.map { colName =>
        val idx = schema.indexOf(colName).getOrElse {
          throw new IllegalArgumentException(s"列名 '$colName' 不存在")
        }
        row.get(idx)
      }
      Row.fromSeq(selectedValues)
    }
    DefaultDataFrame(selectedSchema, ClosableIterator(selectedStream)(stream.close()))
  }

  override def limit(n: Int): DataFrame = {
    DefaultDataFrame(schema, ClosableIterator(stream.take(n))(stream.close()))
  }

  override def foreach(f: Row => Unit): Unit = ResourceUtils.using(stream) { iter => iter.foreach(f(_)) }

  override def collect(): List[Row] = ResourceUtils.using(stream) {
    _.toList
  }

  override def mapIterator[T](f: ClosableIterator[Row] => T): T = f(stream)

  override def getDataFrameStatistic: DataFrameStatistics = dataFrameStatistics
}

object DefaultDataFrame {
  def apply(schema: StructType,
            stream: Iterator[Row]): DefaultDataFrame = {
    new DefaultDataFrame(schema, ClosableIterator(stream)())
  }
}
