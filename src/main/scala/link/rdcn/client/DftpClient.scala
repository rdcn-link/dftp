package link.rdcn.client

import link.rdcn.client.ClientUtils.convertStructTypeToArrowSchema
import link.rdcn.operation._
import link.rdcn.server.ArrowFlightStreamWriter
import link.rdcn.struct._
import link.rdcn.user.Credentials
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.auth.ClientAuthHandler
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}

import java.io.{FileInputStream, InputStream, InputStreamReader}
import java.nio.file.Paths
import java.util.Properties
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 19:47
 * @Modified By:
 */
class DftpClient(host: String, port: Int, useTLS: Boolean = false) {
  val prefixSchema: String = "dftp"

  def login(credentials: Credentials): Unit = {
    flightClient.authenticate(new FlightClientAuthHandler(credentials))
  }

  def doAction(actionName: String, params: Array[Byte] = Array.emptyByteArray, paramMap: Map[String, Any] = Map.empty): DataFrame = {
    val body = CodecUtils.encodeWithMap(params, paramMap)
    val actionResult = flightClient.doAction(new Action(actionName, body))
    ClientUtils.parseFlightActionResults(actionResult, allocator)
  }

  def get(url: String): DataFrame = {
    val urlValidator = new UrlValidator(prefixSchema)
    if (urlValidator.isPath(url)) RemoteDataFrameProxy(SourceOp(url), getRows) else {
      urlValidator.validate(url) match {
        case Right((host, port, path)) => {
          if (host == this.host && port.getOrElse(3101) == this.port)
            RemoteDataFrameProxy(SourceOp(url), getRows)
          else throw new IllegalArgumentException(s"Invalid request URL: $url  Expected format: $prefixSchema://${this.host}[:${this.port}]")
        }
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }
  }

  def put(dataFrame: DataFrame, setDataBatchLen: Int = 100): String = {
    val arrowSchema = convertStructTypeToArrowSchema(dataFrame.schema)
    val childAllocator = allocator.newChildAllocator("put-data-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, childAllocator)
    val putListener = new SyncPutListener
    val writer = flightClient.startPut(FlightDescriptor.path(""), root, putListener)
    val loader = new VectorLoader(root)
    dataFrame.mapIterator(iter => {
      val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
      try {
        arrowFlightStreamWriter.process(root, setDataBatchLen).foreach(batch => {
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
        ClientUtils.parsePutListenerToJson(putListener).getOrElse("")
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

  protected def getRows(operationNode: String): (StructType, ClosableIterator[Row]) = {
    val schemaAndIter = getStream(flightClient, new Ticket(CodecUtils.encodeTicket(GET_STREAM, operationNode)))
    val stream = schemaAndIter._2.map(seq => Row.fromSeq(seq))
    (schemaAndIter._1, ClosableIterator(stream)())
  }

  private def getStream(flightClient: FlightClient, ticket: Ticket): (StructType, Iterator[Seq[Any]]) = {
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
                  val blobId = CodecUtils.decodeString(v.get(index))
                  val blobTicket = new Ticket(CodecUtils.encodeTicket(BLOB_STREAM, SourceOp(blobId).toJson.toString()))
                  val blob = new Blob {
                    val iter = getStream(flightClient, blobTicket)._2
                    val chunkIterator = iter.map(value => {
                      value.head match {
                        case v: Array[Byte] => v
                        case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
                      }
                    })

                    override def offerStream[T](consume: InputStream => T): T = {
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
    (schema, iter)
  }

  private val BLOB_STREAM: Byte = 1
  private val GET_STREAM: Byte = 2

  private val location = {
    if (useTLS) {
      val props = new Properties()
      val confPathURI = this.getClass.getProtectionDomain().getCodeSource().getLocation().toURI
      val fis = new InputStreamReader(new FileInputStream(Paths.get(confPathURI).resolve("user.conf").toString), "UTF-8")
      try props.load(fis) finally fis.close()
      System.setProperty("javax.net.ssl.trustStore", Paths.get(props.getProperty("tls.path")).toString())
      Location.forGrpcTls(host, port)
    } else
      Location.forGrpcInsecure(host, port)
  }
  private val allocator: BufferAllocator = new RootAllocator()
  private val flightClient: FlightClient = FlightClient.builder(allocator, location).build()

  private class FlightClientAuthHandler(credentials: Credentials) extends ClientAuthHandler {

    private var callToken: Array[Byte] = _

    override def authenticate(clientAuthSender: ClientAuthHandler.ClientAuthSender, iterator: java.util.Iterator[Array[Byte]]): Unit = {
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

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DftpClient = {
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3)
        client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }

  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DftpClient = {
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3, true)
        client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }
}
