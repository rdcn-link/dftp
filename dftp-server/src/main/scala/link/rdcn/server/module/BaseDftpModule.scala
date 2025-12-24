package link.rdcn.server.module

import link.rdcn.client.UrlValidator
import link.rdcn.message.GetStreamType
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.{BlobRegistry, DataFrame, DataFrameShape, DefaultDataFrame, Row, StructType}
import link.rdcn.user.UserPrincipal
import link.rdcn.util.DataUtils
import org.json.JSONObject

import java.util.UUID
import scala.collection.concurrent.TrieMap

class BaseDftpModule extends DftpModule {

  //TODO: should all data frame providers be registered?
  private val dataFrameHolder = new Workers[DataFrameProviderService]
  private implicit var serverContext: ServerContext = _
  private val dftpBaseEventHandler = new EventHandler {

    val streamCache = TrieMap[String, DataFrame]()
    val streamExpiryDateCache = TrieMap[String, Long]()

    override def accepts(event: CrossModuleEvent): Boolean = {
      event match {
        case _: CollectGetStreamMethodEvent => true
        case _: CollectActionMethodEvent => true
        case _ => false
      }
    }

    def cacheDataFrame(dataFrame: DataFrame): JSONObject = {
      val ticketId: String = UUID.randomUUID().toString
      streamCache.put(ticketId, dataFrame)
      streamExpiryDateCache.put(ticketId, System.currentTimeMillis() + 60 * 1000) //default 1 minute
      val responseJsonObject = new JSONObject()
      responseJsonObject.put("shapeName", DataFrameShape.Tabular.name)
        .put("schema", dataFrame.schema.toString)
        .put("dftpTicket", ticketId)
    }

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case require: CollectActionMethodEvent =>
          require.collect(new ActionMethod {
            override def accepts(request: DftpActionRequest): Boolean =
              request.getJonsObjectRequest().getString("actionType") == "getDataFrameMeta"

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              val requestJsonObject = request.getJonsObjectRequest()
              val streamType = GetStreamType.fromString(requestJsonObject.getString("getStreamType"))
              streamType match {
                case GetStreamType.Get =>
                  try{
                    val transformOp = TransformOp.fromJsonString(requestJsonObject.getString("streamPayLoad"))
                    val dataFrame = transformOp.execute(new ExecutionContext {
                      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
                        Some(dataFrameHolder.work(new TaskRunner[DataFrameProviderService, DataFrame] {

                          override def acceptedBy(worker: DataFrameProviderService): Boolean = worker.accepts(dataFrameNameUrl)

                          override def executeWith(worker: DataFrameProviderService): DataFrame = worker.getDataFrame(dataFrameNameUrl, request.getUserPrincipal())

                          override def handleFailure(): DataFrame = throw new DataFrameNotFoundException(dataFrameNameUrl)
                        }))
                      }
                    })
                    response.sendJsonObject(cacheDataFrame(dataFrame))
                  }catch {
                    case e: DataFrameAccessDeniedException => response.sendError(403, e.getMessage)
                      throw e
                    case e: DataFrameNotFoundException =>
                      response.sendError(404, e.getMessage)
                      throw e
                    case e: Exception => response.sendError(500, e.getMessage)
                      throw e
                  }
                case GetStreamType.Blob =>
                  val blobId = requestJsonObject.getString("streamPayLoad")
                  val blob = BlobRegistry.getBlob(blobId)
                  if (blob.isEmpty) {
                    response.sendError(404, s"blob ${blobId} resource closed")
                  } else {
                    blob.get.offerStream(inputStream => {
                      val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream)
                        .map(bytes => Row.fromSeq(Seq(bytes)))
                      val schema = StructType.blobStreamStructType
                      response.sendJsonObject(cacheDataFrame(DefaultDataFrame(schema, stream)))
                    })
                  }

              }
            }
          })

        case require: CollectGetStreamMethodEvent =>
          require.collect(
            new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean = {
                streamCache.keys.toList.contains(request.getTicket)
              }

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                val ticketId = request.getTicket
                val expiryTime = streamExpiryDateCache.get(ticketId)
                if(expiryTime.isEmpty){
                  response.sendError(500, "ticket without invalid expiration time")
                }else if(expiryTime.get > System.currentTimeMillis()) {
                  response.sendError(403, s"ticket $ticketId has expired")
                }else {
                  val dataFrame = streamCache.get(ticketId).get
                  response.sendDataFrame(dataFrame)
                }
              }
            })
        case _ =>
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    anchor.hook(dftpBaseEventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(CollectDataFrameProviderEvent(dataFrameHolder))
    })
  }

  override def destroy(): Unit = {
  }
}

trait DataFrameProviderService {
  def accepts(dataFrameUrl: String): Boolean

  def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame
}

case class CollectDataFrameProviderEvent(holder: Workers[DataFrameProviderService]) extends CrossModuleEvent {
  def collect = holder.add(_)
}

