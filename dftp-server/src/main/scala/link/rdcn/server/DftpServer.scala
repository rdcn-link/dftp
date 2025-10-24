package link.rdcn.server

import link.rdcn.struct.{ArrowFlightStreamWriter, BlobRegistry, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}
import ServerUtils.convertStructTypeToArrowSchema
import link.rdcn.util.{CodecUtils, DataUtils}
import link.rdcn.{DftpConfig, Logging}
import link.rdcn.client.UrlValidator
import link.rdcn.log.{AccessLogger, FileAccessLogger}
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server.module.Module
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

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:31
 * @Modified By:
 */
class DftpServer(userAuthenticationService: AuthenticationService, module: DftpMethodService) extends Logging
{

  def setProtocolSchema(protocolSchema: String): DftpServer = {
    this.protocolScheme = protocolSchema
    this
  }

  def enableTLS(tlsCertFile: File, tlsKeyFile: File): DftpServer = {
    this.useTls = true
    this.tlsCertFile = tlsCertFile
    this.tlsKeyFile = tlsKeyFile
    this
  }

  def disableTLS(): DftpServer = {
    this.useTls = false
    this
  }

  protected def parseTicket(bytes: Array[Byte]): DftpTicket = {
    val BLOB_TICKET: Byte = 1
    val GET_TICKET: Byte = 2

    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val typeId: Byte = buffer.get()
    val len = buffer.getInt()
    val b = new Array[Byte](len)
    buffer.get(b)
    val ticketContent = new String(b, StandardCharsets.UTF_8)
    typeId match {
      case BLOB_TICKET => BlobTicket(ticketContent)
      case GET_TICKET => GetTicket(ticketContent)
      case _ => throw new Exception("parse fail")
    }
  }

  protected def getStreamByTicket(userPrincipal: UserPrincipal,
                                  ticket: Array[Byte],
                                  response: GetResponse): Unit =
  {
    val startTime = System.currentTimeMillis()
    parseTicket(ticket) match {
      case BlobTicket(ticketContent) =>
        val blobId = ticketContent
        val blob = BlobRegistry.getBlob(blobId)
        if (blob.isEmpty)
        {
          accessLogger.logError("-", "-", startTime.toString, "STREAM", s"blob-$blobId"
            , 404, s"blob ${blobId} resource closed", System.currentTimeMillis() - startTime)
          response.sendError(404, s"blob ${blobId} resource closed")
        } else
        {
          accessLogger.logAccess("-", "-", startTime.toString, "STREAM", s"blob-$blobId"
            , 200, -1, System.currentTimeMillis() - startTime)
          blob.get.offerStream(inputStream => {
            val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream)
              .map(bytes => Row.fromSeq(Seq(bytes)))
            val schema = StructType.blobStreamStructType
            response.sendDataFrame(DefaultDataFrame(schema, stream))
          })
        }
      case  GetTicket(ticketContent) =>
        val transformOp = TransformOp.fromJsonString(ticketContent)
        require(transformOp.sourceUrlList.size == 1)
        val dataFrameUrl = transformOp.sourceUrlList.head
        val urlValidator = UrlValidator(protocolScheme)
        val urlAndPath = urlValidator.validate(dataFrameUrl) match {
          case Right(v) => (dataFrameUrl, v._3)
          case Left(message) => (s"$protocolScheme://${dftpConfig.host}:${dftpConfig.port}${dataFrameUrl}", dataFrameUrl)
        }
        val getRequest = new GetRequest {
          override def getRequestURI(): String = urlAndPath._2

          override def getRequestURL(): String = urlAndPath._1

          override def getUserPrincipal(): UserPrincipal = userPrincipal
        }
        val getResponse = new GetResponse {
          override def sendDataFrame(inDataFrame: DataFrame): Unit = {
            accessLogger.logAccess("-", "_", startTime.toString, "STREAM", urlAndPath._2
              , 200, -1, System.currentTimeMillis() - startTime)
            val outDataFrame = transformOp.execute(new ExecutionContext {
              override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] =
                Some(inDataFrame)
            })
            response.sendDataFrame(outDataFrame)
          }

          override def sendError(code: Int, message: String): Unit = {
            accessLogger.logError("-", "-", startTime.toString, "STREAM", urlAndPath._2
              ,code , message, System.currentTimeMillis() - startTime)
            response.sendError(code, message)
          }
        }
        module.doGet(getRequest, getResponse)
      case other => response.sendError(400, s"illegal ticket $other")
    }
  }

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

  private val authenticatedUserMap = new ConcurrentHashMap[String, UserPrincipal]()

  private var protocolScheme: String = "dftp"
  private var location: Location = _

  private var useTls: Boolean = false
  private var tlsCertFile: File = _
  private var tlsKeyFile: File = _

  private var dftpConfig: DftpConfig = _

  protected var accessLogger: AccessLogger = _

  @volatile private var allocator: BufferAllocator = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  private def buildServer(dftpConfig: DftpConfig): Unit = {
    this.dftpConfig = dftpConfig
    if(dftpConfig.accessLoggerType == "file")
      accessLogger = new FileAccessLogger(dftpConfig)
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
        val authenticatedUser = DftpServer.this.userAuthenticationService.authenticate(cred)
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

    override def doAction(context: FlightProducer.CallContext,
                          action: Action,
                          listener: FlightProducer.StreamListener[Result]): Unit =
    {
      val startTime = System.currentTimeMillis()
      val actionResponse = new ActionResponse {
        override def send(data: Array[Byte]): Unit = {
          accessLogger.logAccess("-", "-", startTime.toString, "ACTION", action.getType
            , 200, data.length, System.currentTimeMillis() - startTime)
          listener.onNext(new Result(data))
          listener.onCompleted()
        }

        override def sendError(code: Int, message: String): Unit = {
          accessLogger.logError("-", "-", startTime.toString, "ACTION", action.getType
          , code, message, System.currentTimeMillis() - startTime)
          sendErrorWithFlightStatus(code, message)
        }
      }
      val body = ActionBody.decode(action.getBody)
      val actionRequest = new ActionRequest {
        override def getActionName(): String = action.getType

        override def getParameter(): Array[Byte] = body._1

        override def getParameterAsMap(): Map[String, Any] = body._2

        override def getUserPrincipal(): UserPrincipal =
          authenticatedUserMap.get(context.peerIdentity())
      }
      module.doAction(actionRequest, actionResponse)
    }

    override def getStream(context: FlightProducer.CallContext,
                           ticket: Ticket,
                           listener: FlightProducer.ServerStreamListener): Unit =

    {
      val startTime = System.currentTimeMillis()
      val dataBatchLen = 1000
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
              arrowFlightStreamWriter.process(root, dataBatchLen).foreach(batch => {
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
              iter.close()
              if (root != null) root.close()
              if (childAllocator != null) childAllocator.close()
            }
          })
        }

        override def sendError(code: Int, message: String): Unit = {
          accessLogger.logError("-", "-", startTime.toString, "PUT", "put"
            , code, message, System.currentTimeMillis() - startTime)
          sendErrorWithFlightStatus(code, message)
        }
      }
      val authenticatedUser = authenticatedUserMap.get(context.peerIdentity())
      getStreamByTicket(authenticatedUser, ticket.getBytes, response)
    }

    override def acceptPut(
                            context: FlightProducer.CallContext,
                            flightStream: FlightStream,
                            ackStream: FlightProducer.StreamListener[PutResult]
                          ): Runnable =
    {
      new Runnable {
        override def run(): Unit = {
          val startTime = System.currentTimeMillis()
          val request = new PutRequest {
            override def getDataFrame(): DataFrame = {
              accessLogger.logAccess("-", "_", startTime.toString, "PUT", "put"
                , 200, -1, System.currentTimeMillis() - startTime)
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
            override def send(data: Array[Byte]): Unit = {
              try {
                val buf: ArrowBuf = allocator.buffer(data.length)
                try {
                  buf.writeBytes(data)
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
          module.doPut(request, response)
        }
      }
    }

    override def getFlightInfo(context: FlightProducer.CallContext,
                               descriptor: FlightDescriptor): FlightInfo =
    {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val schema = new Schema(List.empty.asJava)
      new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
    }

    override def listFlights(context: FlightProducer.CallContext,
                             criteria: Criteria,
                             listener: FlightProducer.StreamListener[FlightInfo]): Unit =
    {
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

class NullDftpMethodService extends DftpMethodService {
  override def doGet(request: GetRequest, response: GetResponse): Unit = {
    response.sendError(404, s"resource ${request.getRequestURI()} not found")
  }

  override def doPut(request: PutRequest, putResponse: PutResponse): Unit = {
    putResponse.sendError(200, s"success")
  }

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = {
    response.sendError(404, s"Action ${request.getActionName()} not found")
  }
}

class CredentialsProvider extends AuthenticationService {
  override def authenticate(credentials: Credentials): UserPrincipal =
    UserPrincipalWithCredentials(credentials)
}

