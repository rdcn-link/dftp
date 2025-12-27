package link.rdcn.server

import link.rdcn.server.exception.{TicketExpiryException, TicketNotFoundException}
import link.rdcn.struct.{Blob, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.util.DataUtils

import java.util.UUID
import scala.collection.concurrent.TrieMap

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/25 17:48
 * @Modified By:
 */
object ServedDataFramePool {

  private val dataFrameCache = TrieMap[String, DataFrame]()
  private val ticketExpiryDateCache = TrieMap[String, Long]()

  def registry(dataFrame: DataFrame, expiryDate: Long = -1L): String = {
    val dataFrameId = UUID.randomUUID().toString
    dataFrameCache.put(dataFrameId, dataFrame)
    ticketExpiryDateCache.put(dataFrameId, expiryDate)
    dataFrameId
  }

  def registry(blob: Blob, expiryDate: Long = -1L): String = {
    val dataFrameId = UUID.randomUUID().toString
    val dataFrame = blob.offerStream[DataFrame](inputStream => {
      val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream)
        .map(bytes => Row.fromSeq(Seq(bytes)))
      val schema = StructType.blobStreamStructType
      DefaultDataFrame(schema, stream)
    })
    dataFrameCache.put(dataFrameId, dataFrame)
    ticketExpiryDateCache.put(dataFrameId, expiryDate)
    dataFrameId
  }

  def exists(ticketId: String): Boolean = {
    dataFrameCache.keys.toList.contains(ticketId)
  }

  def getDataFrame(ticketId: String): Option[DataFrame] = {
    val expiryDate = ticketExpiryDateCache.get(ticketId)
    if(expiryDate.isEmpty) throw new TicketNotFoundException(ticketId)
    else if(expiryDate.get < System.currentTimeMillis() && expiryDate.get != -1L) {
      throw new TicketExpiryException(ticketId, expiryDate.get)
    }else dataFrameCache.get(ticketId)
  }

  def cleanUp(): Unit = {
    dataFrameCache.values.foreach(df => df.mapIterator[Unit](iter => iter.close()))
    dataFrameCache.clear()
    ticketExpiryDateCache.clear()
  }

}
