package link.rdcn.server.dftp

import link.rdcn.struct.ValueType.BinaryType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticatedProvider, AuthenticatedUser, AuthenticatedUserWithCredentials, Credentials}
import link.rdcn.server.ServerUtils.convertStructTypeToArrowSchema
import link.rdcn.util.{CodecUtils, DataUtils}
import link.rdcn.Logging
import link.rdcn.DftpConfig
import link.rdcn.operation.{ExecutionContext, Operation, SourceOp}
import link.rdcn.server.{ActionRequest, ActionResponse, ArrowFlightStreamWriter, BlobRegistry, DataFrameWithArrowRoot, DftpServiceHandler, GetRequest, GetResponse, PutRequest, PutResponse, ServerUtils}
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.flight.{Action, CallStatus, Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, FlightProducer, FlightServer, FlightStream, Location, NoOpFlightProducer, PutResult, Result, Ticket}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}

import java.io.File
import java.nio.charset.StandardCharsets
import java.util
import java.util.{Optional, UUID}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:31
 * @Modified By:
 */

class NullDftpServiceHandler extends DftpServiceHandler {
  override def doGet(request: GetRequest, response: GetResponse): Unit = {
    response.sendError(404, s"resource ${request.getRequestedPath()} not found")
  }

  override def doPut(request: PutRequest, putResponse: PutResponse): Unit = {
    putResponse.sendError(200, s"success")
  }

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = {
    response.sendError(404, s"Action ${request.getActionName()} not found")
  }
}

class CredentialsProvider extends AuthenticatedProvider {
  override def authenticate(credentials: Credentials): AuthenticatedUser = AuthenticatedUserWithCredentials(credentials)
}

class DftpServer {

  def setAuthHandler(authenticatedProvider: AuthenticatedProvider): DftpServer = {
    this.authenticatedProvider = authenticatedProvider
    this
  }

  def setServiceHandler(dftpServiceHandler: DftpServiceHandler): DftpServer = {
    this.dftpServiceHandler = dftpServiceHandler
    this
  }

  def setProtocolSchema(protocolSchema: String): DftpServer = {
    this.protocolSchema = protocolSchema
    this
  }

  def setTLS(tlsCertFile: File, tlsKeyFile: File): DftpServer = {
    this.useTls = true
    this.tlsCertFile = tlsCertFile
    this.tlsKeyFile = tlsKeyFile
    this
  }

  def buildStream(authenticatedUser: AuthenticatedUser, ticket: Array[Byte]): Either[DataFrame, (Int, String)] = {
    val ticketInfo = CodecUtils.decodeTicket(ticket)
    if (ticketInfo._1 == BLOB_STREAM) {
      val blobId = ticketInfo._2.asInstanceOf[SourceOp].dataFrameUrl
      val blob = BlobRegistry.getBlob(blobId)
      if (blob.isEmpty) {
        Right(404, s"blob ${blobId} resource closed")
      }
      else {
        blob.get.offerStream(inputStream => {
          val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream).map(bytes => Row.fromSeq(Seq(bytes)))
          val schema = StructType.empty.add("content", BinaryType)
          Left(DefaultDataFrame(schema, stream))
        })
      }
    } else {
      val sourceList = new ListBuffer[String]
      val operation = Operation.fromJsonString(ticketInfo._2, sourceList)
      var result: Option[Either[DataFrame, (Int, String)]] = None
      val resultDataFrame = operation.execute(new ExecutionContext{
        override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
          var df: Option[DataFrame] = None
          val getRequest: GetRequest = new GetRequest {
            override def getRequestedPath(): String = dataFrameNameUrl

            override def getRequestAuthenticated(): AuthenticatedUser = authenticatedUser
          }
          val getResponse: GetResponse = new GetResponse {
            override def sendDataFrame(dataFrame: DataFrame): Unit = df = Some(dataFrame)

            override def sendError(code: Int, message: String): Unit = {
              result = Some(Right(code, message))
            }
          }
          dftpServiceHandler.doGet(getRequest, getResponse)
          df
        }
      })
      if(result.nonEmpty) result.get else Left(resultDataFrame)
    }
  }

  def authenticate(credentials: Credentials): AuthenticatedUser = this.authenticatedProvider.authenticate(credentials)

  def start(dftpConfig: DftpConfig): Unit = synchronized {
    if (started) return
    buildServer(dftpConfig)
    serverThread = new Thread(() => {
      try {
        flightServer.start()
        started = true
        Runtime.getRuntime.addShutdownHook(new Thread(() => {
          close()
        }))
        flightServer.awaitTermination()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        started = false
      }
    })
    serverThread.setDaemon(false)
    serverThread.start()
  }

  def close(): Unit = synchronized {
    if (!started) return
    try {
      if (flightServer != null) flightServer.close()
    } catch {
      case _: Throwable => // ignore
    }

    try {
      if (allocator != null) allocator.close()
    } catch {
      case _: Throwable => // ignore
    }

    if (serverThread != null && serverThread.isAlive) {
      serverThread.interrupt()
    }

    // reset
    flightServer = null
    allocator = null
    serverThread = null
    started = false
  }

  private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()
  private var authenticatedProvider: AuthenticatedProvider = new CredentialsProvider
  private var dftpServiceHandler: DftpServiceHandler = new NullDftpServiceHandler
  private var protocolSchema: String = "dftp"
  private var location: Location = _

  private val BLOB_STREAM: Byte = 1
  private val GET_STREAM: Byte = 2

  private var useTls: Boolean = false
  private var tlsCertFile: File = _
  private var tlsKeyFile: File = _

  @volatile private var allocator: BufferAllocator = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  private def buildServer(dftpConfig: DftpConfig): Unit = {
    location = if (useTls)
      Location.forGrpcTls(dftpConfig.host, dftpConfig.port)
    else
      Location.forGrpcInsecure(dftpConfig.host, dftpConfig.port)

    allocator = new RootAllocator()

    val producer = new DftpFlightProducer()

    if (useTls) {
      flightServer = FlightServer.builder(allocator, location, producer)
        .useTls(tlsCertFile, tlsKeyFile)
        .authHandler(new FlightServerAuthHandler)
        .build()
    } else {
      flightServer = FlightServer.builder(allocator, location, producer)
        .authHandler(new FlightServerAuthHandler)
        .build()
    }
  }

  private class FlightServerAuthHandler extends ServerAuthHandler {
    override def authenticate(serverAuthSender: ServerAuthHandler.ServerAuthSender, iterator: util.Iterator[Array[Byte]]): Boolean = {
      try {
        val cred = CodecUtils.decodeCredentials(iterator.next())
        val authenticatedUser = DftpServer.this.authenticate(cred)
        val token = UUID.randomUUID().toString()
        authenticatedUserMap.put(token, authenticatedUser)
        serverAuthSender.send(CodecUtils.encodeString(token))
        true
      } catch {
        case e: Exception => false
      }
    }

    override def isValid(bytes: Array[Byte]): Optional[String] = {
      val tokenStr = CodecUtils.decodeString(bytes)
      Optional.of(tokenStr)
    }
  }

  private class DftpFlightProducer extends NoOpFlightProducer with Logging {

    override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
      val actionResponse = new ActionResponse {
        override def sendMessage(jsonStr: String): Unit = ServerUtils.sendJsonString(jsonStr, listener, allocator)

        override def sendError(code: Int, message: String): Unit = sendErrorWithFlightStatus(code, message)
      }
      val body = CodecUtils.decodeWithMap(action.getBody)
      val actionRequest = new ActionRequest {
        override def getActionName(): String = action.getType

        override def getActionParameters(): Array[Byte] = body._1

        override def getActionParameterMap(): Map[String, Any] = body._2
      }
      dftpServiceHandler.doAction(actionRequest, actionResponse)
    }

    override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
      val setDataBatchLen = 1000
      val response = new GetResponse {
        override def sendDataFrame(dataFrame: DataFrame): Unit = {
          val schema = convertStructTypeToArrowSchema(dataFrame.schema)
          val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
          val root = VectorSchemaRoot.create(schema, childAllocator)
          val loader = new VectorLoader(root)
          listener.start(root)
          dataFrame.mapIterator(iter => {
            val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
            try {
              arrowFlightStreamWriter.process(root, setDataBatchLen).foreach(batch => {
                try {
                  loader.load(batch)
                  while (!listener.isReady()) {
                    LockSupport.parkNanos(1)
                  }
                  listener.putNext()
                } finally {
                  batch.close()
                }
              })
              listener.completed()
            } catch {
              case e: Throwable => listener.error(e)
                e.printStackTrace()
                throw e
            } finally {
              if (root != null) root.close()
              if (childAllocator != null) childAllocator.close()
            }
          })
        }

        override def sendError(code: Int, message: String): Unit = sendErrorWithFlightStatus(code, message)
      }
      val authenticatedUser = authenticatedUserMap.get(context.peerIdentity())
      buildStream(authenticatedUser, ticket.getBytes) match {
        case Left(dataFrame) => response.sendDataFrame(dataFrame)
        case Right((code, message)) => response.sendError(code, message)
      }
    }

    override def acceptPut(
                            context: FlightProducer.CallContext,
                            flightStream: FlightStream,
                            ackStream: FlightProducer.StreamListener[PutResult]
                          ): Runnable = {
      new Runnable {
        override def run(): Unit = {
          val request = new PutRequest {
            override def getDataFrame(): DataFrame = {
              var schema = StructType.empty
              if (flightStream.next()) {
                val root = flightStream.getRoot
                schema = ServerUtils.arrowSchemaToStructType(root.getSchema)
                val stream = ServerUtils.flightStreamToRowIterator(flightStream)
                new DataFrameWithArrowRoot(root, schema, stream)
              } else {
                DefaultDataFrame(schema, Iterator.empty)
              }
            }
          }
          val response = new PutResponse {
            override def sendMessage(jsonStr: String): Unit = {
              try {
                val bytes = CodecUtils.encodeString(jsonStr)
                val buf: ArrowBuf = allocator.buffer(bytes.length)
                try {
                  buf.writeBytes(bytes)
                  ackStream.onNext(PutResult.metadata(buf))
                } finally {
                  buf.close()
                }
                ackStream.onCompleted()
              } catch {
                case e: Throwable =>
                  e.printStackTrace()
                  ackStream.onError(e)
              }
            }

            override def sendError(code: Int, message: String): Unit = sendErrorWithFlightStatus(code, message)
          }
          dftpServiceHandler.doPut(request, response)
        }
      }
    }

    override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val schema = new Schema(List.empty.asJava)
      new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
    }

    override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
      listener.onCompleted()
    }

    /**
     * 400 Bad Request → 请求参数非法，对应 INVALID_ARGUMENT
     * 401 Unauthorized → 未认证，对应 UNAUTHENTICATED
     * 403 Forbidden → 没有权限，Flight 没有 PERMISSION_DENIED，用 UNAUTHORIZED 替代
     * 404 Not Found → 资源未找到，对应 NOT_FOUND
     * 408 Request Timeout → 请求超时，对应 TIMED_OUT
     * 409 Conflict → 冲突，比如资源已存在，对应 ALREADY_EXISTS
     * 500 Internal Server Error → 服务端内部错误，对应 INTERNAL
     * 501 Not Implemented → 未实现的功能，对应 UNIMPLEMENTED
     * 503 Service Unavailable → 服务不可用（可能是过载或维护），对应 UNAVAILABLE
     * 其它未知错误 → 映射为 UNKNOWN
     * */
    private def sendErrorWithFlightStatus(code: Int, message: String): Unit = {
      val status = code match {
        case 400 => CallStatus.INVALID_ARGUMENT
        case 401 => CallStatus.UNAUTHENTICATED
        case 403 => CallStatus.UNAUTHORIZED
        case 404 => CallStatus.NOT_FOUND
        case 408 => CallStatus.TIMED_OUT
        case 409 => CallStatus.ALREADY_EXISTS
        case 500 => CallStatus.INTERNAL
        case 501 => CallStatus.UNIMPLEMENTED
        case 503 => CallStatus.UNAVAILABLE
        case _ => CallStatus.UNKNOWN
      }
      throw status.withDescription(message).toRuntimeException
    }
  }
}

