package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.client.ClientUtils.convertStructTypeToArrowSchema
import link.rdcn.message.{DftpTicket, GetStreamType, MapSerializer}
import link.rdcn.operation._
import link.rdcn.struct._
import link.rdcn.user.Credentials
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.auth.ClientAuthHandler
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.json.JSONObject

import java.io.{File, InputStream}
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.{asScalaBufferConverter, collectionAsScalaIterableConverter}
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

  def doAction(actionName: String, paramMap: Map[String, Any] = Map.empty): Array[Byte] = {
    val body = MapSerializer.encodeMap(paramMap)
    val actionResultIter = flightClient.doAction(new Action(actionName, body))
    try {
      actionResultIter.next().getBody
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Sends an action request to the faird server and returns the result.
   *
   * @param action JSON string representing the action to be executed on the server
   * @return JSON string containing the action execution result returned by the server
   */
  def doAction(action: String): String = {
    val actionResultIter = flightClient.doAction(new Action(action))
    CodecUtils.decodeString(actionResultIter.next().getBody)
  }

  protected def getDataFrameMeta(streamPayLoad: String, getStreamType: GetStreamType): DataFrameMeta = {
    val requestJson = new JSONObject()
    requestJson.put("streamPayLoad", streamPayLoad)
    requestJson.put("getStreamType", getStreamType.name)
    requestJson.put("actionType", "getDataFrameMeta")
    val responseJson = new JSONObject(doAction(requestJson.toString))
    new DataFrameMeta {
      override def getDataFrameShape: DataFrameShape =
        DataFrameShape.fromName(responseJson.getString("shapeName"))

      override def getDataFrameSchema: StructType =
        StructType.fromString(responseJson.getString("schema"))

      override def getStreamTicket: DftpTicket =
        DftpTicket(responseJson.getString("dftpTicket"))
    }
  }

  def getDataFrameMeta(url: String): DataFrameMeta =
    getDataFrameMeta(SourceOp(url).toJsonString, GetStreamType.Get)

  def get(url: String): DataFrame = {
    if (UrlValidator.isPath(url)) {
      RemoteDataFrameProxy(SourceOp(url), getStream, getDataFrameMeta)
    } else {
      UrlValidator.validate(url) match {
        case Right((prefixSchema, host, port, path)) => {
          if (host == this.host && port.getOrElse(3101) == this.port)
            RemoteDataFrameProxy(SourceOp(url), getStream, getDataFrameMeta)
          else throw new IllegalArgumentException(s"Invalid request URL: $url  Expected format: $prefixSchema://${this.host}[:${this.port}]")
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

  def getStream(dftpTicket: DftpTicket): Iterator[Row] = {
    val flightStream = flightClient.getStream(dftpTicket.ticket)
    val vectorSchemaRootReceived = flightStream.getRoot
//    val schema = ClientUtils.arrowSchemaToStructType(vectorSchemaRootReceived.getSchema)
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
                  val blobTicket = getDataFrameMeta(blobId, GetStreamType.Blob).getStreamTicket
                  val blob = new Blob {
                    override def offerStream[T](consume: InputStream => T): T = {
                      val iter = getStream(blobTicket)
                      val chunkIterator = iter.map(value => {
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
