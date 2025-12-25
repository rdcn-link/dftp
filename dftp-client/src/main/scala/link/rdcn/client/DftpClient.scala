package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.client.ClientUtils.convertStructTypeToArrowSchema
import link.rdcn.message.{ActionMethodType, DftpTicket}
import link.rdcn.operation._
import link.rdcn.struct._
import link.rdcn.user.Credentials
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.auth.ClientAuthHandler
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.json.JSONObject

import java.io.{File, InputStream}
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 19:47
 * @Modified By:
 */
class DftpClient(host: String, port: Int, useTLS: Boolean = false) extends Logging {

  def login(credentials: Credentials): Unit = {
    flightClient.authenticate(new FlightClientAuthHandler(credentials))
  }

  /**
   * Sends an action request to the faird server and returns the result.
   *
   * @param actionName the name of the action to be executed on the server
   * @param parameters JSON-formatted string representing the parameters of the action
   * @return a JSON-formatted string containing the execution result returned by the server
   */
  def doAction(actionName: String, parameters: String = new JSONObject().toString): String = {
    val actionResultIter = flightClient.doAction(new Action(actionName, CodecUtils.encodeString(parameters)))
    CodecUtils.decodeString(actionResultIter.next().getBody)
  }

  protected def openDataFrame(transformOp: TransformOp): DataFrameDescriptor = {
    val responseJson = new JSONObject(doAction(ActionMethodType.GetTabularMeta.name, transformOp.toJsonString))
    val dataFrameMeta = new DataFrameMeta {
      override def getDataFrameShape: DataFrameShape =
        DataFrameShape.fromName(responseJson.getString("shapeName"))

      override def getDataFrameSchema: StructType =
        StructType.fromString(responseJson.getString("schema"))
    }
    new DataFrameDescriptor {
      override def getDataFrameMeta: DataFrameMeta = dataFrameMeta

      override def getDataFrameTicket: Ticket = DftpTicket.createTicket(responseJson.getString("ticket"))
    }
  }

  def openDataFrame(url: String): DataFrameDescriptor = openDataFrame(SourceOp(url))

  def getBlob(url: String): Blob = {
    val df = RemoteDataFrameProxy(SourceOp(validateUrl(url)), getStream, openDataFrame)
    new Blob {
      override def offerStream[T](consume: InputStream => T): T = {
        val inputStream = df.mapIterator[InputStream](iter => {
          val chunkIterator = iter.map(value => {
            assert(value.values.length == 1)
            value._1 match {
              case v: Array[Byte] => v
              case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
            }
          })
          new IteratorInputStream(chunkIterator)
        })
        consume(inputStream)
      }
    }
  }

  def getTabular(url: String): DataFrame = get(url)

  def get(url: String): DataFrame =
    RemoteDataFrameProxy(SourceOp(validateUrl(url)), getStream, openDataFrame)

  private def validateUrl(url: String): String = {
    if (UrlValidator.isPath(url)) url
    else {
      UrlValidator.validate(url) match {
        case Right((prefixSchema, host, port, path)) => {
          if (host == this.host && port.getOrElse(3101) == this.port) url
          else
            throw new IllegalArgumentException(s"Invalid request URL: $url  Expected format: $prefixSchema://${this.host}[:${this.port}]")
        }
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }
  }

  def put(dataFrame: DataFrame, dataBatchLen: Int = 100): Array[Byte] = {
    val arrowSchema = convertStructTypeToArrowSchema(dataFrame.schema)
    val childAllocator = allocator.newChildAllocator("put-data-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, childAllocator)
    val putListener = new SyncPutListener
    val writer = flightClient.startPut(FlightDescriptor.path(""), root, putListener)
    val loader = new VectorLoader(root)
    dataFrame.mapIterator(iter => {
      val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
      try {
        arrowFlightStreamWriter.process(root, dataBatchLen).foreach(batch => {
          try {
            loader.load(batch)
            while (!writer.isReady()) {
              LockSupport.parkNanos(1)
            }
            writer.putNext()
          } finally {
            batch.close()
          }
        })
        writer.completed()
        ClientUtils.parsePutListener(putListener).getOrElse(Array.empty)
      } catch {
        case e: Throwable => writer.error(e)
          throw e
      } finally {
        if (root != null) root.close()
        if (childAllocator != null) childAllocator.close()
      }
    })
  }

  def close(): Unit = {
    allocator.close()
    flightClient.close()
  }

  def getStream(ticket: Ticket): Iterator[Row] = {
    val flightStream = flightClient.getStream(ticket)
    val vectorSchemaRootReceived = flightStream.getRoot
    val iter: Iterator[Seq[Any]] = new Iterator[Seq[Seq[Any]]] {
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
                  val blobId = CodecUtils.decodeString(v.get(index))
                  val blob = new Blob {
                    override def offerStream[T](consume: InputStream => T): T = {
                      val iter = getStream(DftpTicket.createTicket(blobId))
                      val chunkIterator: Iterator[Array[Byte]] = iter.map(value => {
                        assert(value.values.length == 1)
                        value._1 match {
                          case v: Array[Byte] => v
                          case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
                        }
                      })
                      val stream = new IteratorInputStream(chunkIterator)
                      try consume(stream)
                      finally stream.close()
                    }
                  }
                  (vec.getName, blob)
                }
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }): _*)
          rowMap.values.toList
        })
      }
    }.flatMap(batchRows => batchRows)
    iter.map(seq => Row.fromSeq(seq))
  }

  private val location = {
    if (useTLS) {
      Location.forGrpcTls(host, port)
    } else
      Location.forGrpcInsecure(host, port)
  }

  private val allocator: BufferAllocator = new RootAllocator()
  protected val flightClient: FlightClient = FlightClient.builder(allocator, location).build()

  private class FlightClientAuthHandler(credentials: Credentials) extends ClientAuthHandler {

    private var callToken: Array[Byte] = _

    override def authenticate(clientAuthSender: ClientAuthHandler.ClientAuthSender,
                              iterator: java.util.Iterator[Array[Byte]]): Unit = {
      clientAuthSender.send(CodecUtils.encodeCredentials(credentials))
      try {
        callToken = iterator.next()
      } catch {
        case _: Exception => callToken = null
      }
    }

    override def getCallToken: Array[Byte] = callToken
  }

  private class IteratorInputStream(it: Iterator[Array[Byte]]) extends InputStream {
    private var currentChunk: Array[Byte] = Array.emptyByteArray
    private var index: Int = 0

    /** 从流中读取一个字节 */
    override def read(): Int = {
      if (currentChunk == null) return -1
      if (index >= currentChunk.length) {
        if (it.hasNext) {
          currentChunk = it.next()
          index = 0
          read()
        } else {
          currentChunk = null
          -1
        }
      } else {
        val b = currentChunk(index) & 0xff
        index += 1
        b
      }
    }

    /** 从流中读取多个字节到缓冲区 */
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (currentChunk == null) return -1
      var totalRead = 0
      while (totalRead < len) {
        if (index >= currentChunk.length) {
          if (it.hasNext) {
            currentChunk = it.next()
            index = 0
          } else {
            return if (totalRead == 0) -1 else totalRead
          }
        }
        val bytesToCopy = math.min(len - totalRead, currentChunk.length - index)
        System.arraycopy(currentChunk, index, b, off + totalRead, bytesToCopy)
        index += bytesToCopy
        totalRead += bytesToCopy
      }
      totalRead
    }
  }

  private case class ArrowFlightStreamWriter(stream: Iterator[Row]) {

    def process(root: VectorSchemaRoot, batchSize: Int): Iterator[ArrowRecordBatch] = {
      stream.grouped(batchSize).map(rows => createDummyBatch(root, rows))
    }

    private def createDummyBatch(arrowRoot: VectorSchemaRoot, rows: Seq[Row]): ArrowRecordBatch = {
      arrowRoot.allocateNew()
      val fieldVectors = arrowRoot.getFieldVectors.asScala
      var i = 0
      rows.foreach(row => {
        var j = 0
        fieldVectors.foreach(vec => {
          val value = row.get(j)
          value match {
            case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
            case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
            case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
            case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
            case v: java.math.BigDecimal => vec.asInstanceOf[VarCharVector].setSafe(i, v.toString.getBytes("UTF-8"))
            case v: String =>
              val bytes = v.getBytes("UTF-8")
              vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
            case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
            case v: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(i, v)
            case null => vec.setNull(i)
            case v: DFRef =>
              val bytes = v.url.getBytes("UTF-8")
              vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
            case v: Blob =>
              //TODO Blob chunk transfer default max size 100MB
              val bytes = v.offerStream[Array[Byte]](_.readNBytes(100 * 1024 * 1024))
              vec.asInstanceOf[VarBinaryVector].setSafe(i, bytes)
            case _ => throw new UnsupportedOperationException("Type not supported")
          }
          j += 1
        })
        i += 1
      })
      arrowRoot.setRowCount(rows.length)
      val unloader = new VectorUnloader(arrowRoot)
      unloader.getRecordBatch
    }
  }
}


object DftpClient {
  def connect(url: String, credentials: Credentials = null): DftpClient = {
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3)
        if(credentials != null) client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }

  def connectTLS(url: String, tlsFile: File, credentials: Credentials = null): DftpClient = {
    System.setProperty("javax.net.ssl.trustStore", tlsFile.getAbsolutePath)
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3, true)
        if(credentials != null) client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }
}
