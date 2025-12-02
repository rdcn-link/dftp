package link.rdcn.server

import link.rdcn.Logging
import link.rdcn.server.ServerUtils.convertStructTypeToArrowSchema
import link.rdcn.server.module.KernelModule
import link.rdcn.struct._
import link.rdcn.user.UserPrincipal
import link.rdcn.util.{CodecUtils, DataUtils}
import org.apache.arrow.flight._
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.FileUrlResource

import java.io.File
import java.nio.charset.StandardCharsets
import java.security.{PrivateKey, PublicKey}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import java.util.{Optional, UUID}
import scala.collection.JavaConverters._

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:31
 * @Modified By:
 */

trait ServerContext {
  def getHost(): String
  def getPort(): Int
  def getProtocolScheme(): String
  def getDftpHome(): Option[String]
  def getPublicKeyMap(): Map[String, PublicKey] = Map.empty
  def getPrivateKey: Option[PrivateKey] = None
  def baseUrl: String = s"${getProtocolScheme()}://${getHost()}:${getPort()}"
}

class DftpServer(config: DftpServerConfig) extends Logging {

  private val location = if (config.useTls) {
    Location.forGrpcTls(config.host, config.port)
  } else {
    Location.forGrpcInsecure(config.host, config.port)
  }

  private val kernelModule = new KernelModule()
  protected val modules = new Modules(new ServerContext() {

    override def getHost(): String = config.host

    override def getPort(): Int = config.port

    override def getProtocolScheme(): String = config.protocolScheme

    override def getDftpHome(): Option[String] = config.dftpHome

    override def getPublicKeyMap(): Map[String, PublicKey] = config.pubKeyMap

    override def getPrivateKey: Option[PrivateKey] = config.privateKey
  })

  private val authenticatedUserMap = new ConcurrentHashMap[String, UserPrincipal]()

  @volatile private var allocator: BufferAllocator = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  def startBlocking(): Unit = synchronized {
    if (!started) {
      buildServer()
      try {
        flightServer.start()
        started = true

        Runtime.getRuntime.addShutdownHook(new Thread(() => close()))

        flightServer.awaitTermination()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      } finally {
        started = false
      }
    }
  }

  def start(): Unit = synchronized {
    if (!started) {
      buildServer()
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

  private def buildServer(): Unit = {
    allocator = new RootAllocator()
    val producer = new DftpFlightProducer()

    if (config.useTls) {
      flightServer = FlightServer.builder(allocator, location, producer)
        .useTls(config.tlsCertFile.get, config.tlsKeyFile.get)
        .authHandler(new FlightServerAuthHandler)
        .build()
    } else {
      flightServer = FlightServer.builder(allocator, location, producer)
        .authHandler(new FlightServerAuthHandler)
        .build()
    }

    modules.addModule(kernelModule)
    modules.init()
  }

  private class FlightServerAuthHandler extends ServerAuthHandler {
    override def authenticate(serverAuthSender: ServerAuthHandler.ServerAuthSender, iterator: util.Iterator[Array[Byte]]): Boolean = {
      try {
        val cred = CodecUtils.decodeCredentials(iterator.next())
        val authenticatedUser = kernelModule.authenticate(cred)
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

    override def doAction(callContext: FlightProducer.CallContext,
                          action: Action,
                          listener: FlightProducer.StreamListener[Result]): Unit = {

      val actionResponse = new DftpActionResponse {
        override def sendError(errorCode: Int, message: String): Unit = {
          sendErrorWithFlightStatus(errorCode, message)
        }

        override def sendData(data: Array[Byte]): Unit = {
          listener.onNext(new Result(data))
          listener.onCompleted()
        }
      }

      val actionRequest = new DftpActionRequest {
        override def getActionName(): String = action.getType

        override def getParameter(): Array[Byte] = action.getBody

        override def getUserPrincipal(): UserPrincipal =
          authenticatedUserMap.get(callContext.peerIdentity())
      }

      kernelModule.doAction(actionRequest, actionResponse)
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

    override def getStream(callContext: FlightProducer.CallContext,
                           ticket: Ticket,
                           listener: FlightProducer.ServerStreamListener): Unit = {

      val dataBatchLen = 1000
      val response = new DftpGetStreamResponse {
        override def sendError(code: Int, message: String): Unit = {
          sendErrorWithFlightStatus(code, message)
        }

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
      }

      val authenticatedUser = authenticatedUserMap.get(callContext.peerIdentity())
      val request: DftpGetStreamRequest = kernelModule.parseGetStreamRequest(ticket.getBytes, authenticatedUser)

      kernelModule.getStream(request, response)
    }

    override def acceptPut(
                            callContext: FlightProducer.CallContext,
                            flightStream: FlightStream,
                            ackStream: FlightProducer.StreamListener[PutResult]
                          ): Runnable = {
      new Runnable {
        override def run(): Unit = {
          val request = new DftpPutStreamRequest {
            override def getDataFrame(): DataFrame = {
              var schema = StructType.empty
              if (flightStream.next()) {
                val root = flightStream.getRoot
                schema = ServerUtils.arrowSchemaToStructType(root.getSchema)
                val stream = ServerUtils.flightStreamToRowIterator(flightStream)
                DefaultDataFrame(schema, ClosableIterator(stream)())
              } else {
                DefaultDataFrame(schema, Iterator.empty)
              }
            }

            override def getUserPrincipal(): UserPrincipal = authenticatedUserMap.get(callContext.peerIdentity())
          }

          val response = new DftpPutStreamResponse {
            override def sendError(code: Int, message: String): Unit = sendErrorWithFlightStatus(code, message)

            override def sendData(data: Array[Byte]): Unit =
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

          kernelModule.putStream(request, response)
        }
      }
    }

    override def getFlightInfo(context: FlightProducer.CallContext,
                               descriptor: FlightDescriptor): FlightInfo = {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val schema = new Schema(List.empty.asJava)
      new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
    }

    override def listFlights(context: FlightProducer.CallContext,
                             criteria: Criteria,
                             listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
      listener.onCompleted()
    }
  }
}

object DftpServer {
  def start(config: DftpServerConfig, modulesDefined: Array[DftpModule]): DftpServer = {
    val server = new DftpServer(config) {
      modulesDefined.foreach(modules.addModule(_))
    }

    server.start()
    server
  }

  def start(configXmlFile: File): DftpServer = {
    val server = createDftpServer(configXmlFile)
    server.start()
    server
  }

  def startBlocking(configXmlFile: File): Unit = {
    createDftpServer(configXmlFile).startBlocking()
  }

  private def createDftpServer(configXmlFile: File): DftpServer = {
    val configDir = configXmlFile.getParentFile.getAbsolutePath
    System.setProperty("configDir", configDir)

    val context = new GenericApplicationContext()
    val reader = new XmlBeanDefinitionReader(context)
    reader.loadBeanDefinitions(new FileUrlResource(configXmlFile.getAbsolutePath))
    context.refresh()

    val configBean = context.getBean(classOf[DftpServerConfigBean])
    val config: DftpServerConfig = configBean.toDftpServerConfig

    new DftpServer(config) {
      configBean.modules.foreach(modules.addModule(_))
    }
  }
}