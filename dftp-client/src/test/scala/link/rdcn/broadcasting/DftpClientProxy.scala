/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/16 23:44
 * @Modified By:
 */
package link.rdcn.broadcasting

import link.rdcn.client.{ClientUtils, DftpClient, UrlValidator}
import link.rdcn.server.BlobTicket
import link.rdcn.struct.{Blob, DFRef, StructType}
import link.rdcn.user.Credentials
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.{FlightClient, Ticket}

import java.io.InputStream
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class DftpClientProxy(host:String,port:Int) extends DftpClient(host, port) {

  override def getStream(flightClient: FlightClient, ticket: Ticket): (StructType, Iterator[Seq[Any]]) = {
    val flightStream = flightClient.getStream(ticket)
    val vectorSchemaRootReceived = flightStream.getRoot
    val schema = ClientUtils.arrowSchemaToStructType(vectorSchemaRootReceived.getSchema)
    val iter = new Iterator[Seq[Seq[Any]]] {
      override def hasNext: Boolean = flightStream.next()

      override def next(): Seq[Seq[Any]] = {
        val rowCount = vectorSchemaRootReceived.getRowCount
        val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
        Seq.range(0, rowCount).map(index => {
          val rowMap = mutable.LinkedHashMap(fieldVectors.map(vec => {
            if (vec.isNull(index)) (vec.getName, null)
            else vec match {
              case v: org.apache.arrow.vector.IntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BigIntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.VarCharVector =>
                if (v.getField.getMetadata.isEmpty)
                  (vec.getName, new String(v.get(index)))
                else (vec.getName, DFRef(new String(v.get(index))))
              case v: org.apache.arrow.vector.Float8Vector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BitVector => (vec.getName, v.get(index) == 1)
              case v: org.apache.arrow.vector.VarBinaryVector =>
                if (v.getField.getMetadata.isEmpty) (vec.getName, v.get(index))
                else {
//                                    var promise:Promise[Unit] = null
                  val blobId = CodecUtils.decodeString(v.get(index))
                  val blobTicket = new Ticket(BlobTicket(blobId).encodeTicket())
                  val blob = new Blob {
                    println(s"[${System.currentTimeMillis()}] DEBUG: Server A - Blob created. Connection 2 (to Server B) is now established but LAZY.")
//                                        promise = Promise[Unit]()
                    val iter = getStream(flightClient, blobTicket)._2
                    val chunkIterator = iter.map(value => {
                      value.head match {
                        case v: Array[Byte] => v
                        case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
                      }
                    })

                    override def offerStream[T](consume: InputStream => T): T = {
                      println(s"[${System.currentTimeMillis()}] DEBUG: Server A - Blob.offerStream CALLED. Attempting to pull data from Connection 2 NOW.")
                      val stream = new IteratorInputStream(chunkIterator)
                      try consume(stream)
                      finally {
                        stream.close()
//                                                if(promise != null)
//                                                  promise.success(())
                      }
                    }
                  }
//                                    if(promise != null)
//                                      Await.result(promise.future, 100.second)
                  (vec.getName, blob)
                }
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }): _*)
          rowMap.values.toList
        })
      }
    }.flatMap(batchRows => batchRows)
    (schema, iter)
  }

}

object DftpClientProxy {

    def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DftpClient = {
      UrlValidator.extractBase(url) match {
        case Some(parsed) =>
          val client = new DftpClientProxy(parsed._2, parsed._3)
          client.login(credentials)
          client
        case None =>
          throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
      }
    }

}
