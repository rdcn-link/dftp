package link.rdcn.server.module

import link.rdcn.Logging
import link.rdcn.message.{ActionMethodType}
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameNotFoundException, TicketExpiryException, TicketNotFoundException}
import link.rdcn.struct.{DataFrame, DataFrameShape}
import link.rdcn.user.UserPrincipal
import org.json.JSONObject

class BaseDftpModule extends DftpModule with Logging{

  //TODO: should all data frame providers be registered?
  private val dataFrameHolder = new Workers[DataFrameProviderService]
  private implicit var serverContext: ServerContext = _
  private val dftpBaseEventHandler = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean = {
      event match {
        case _: CollectGetStreamMethodEvent => true
        case _: CollectActionMethodEvent => true
        case _ => false
      }
    }

    def createDataFrameDescriber(dataFrame: DataFrame): JSONObject = {
      val ticketId: String = OpenedDataFrameRegistry.registry(dataFrame)
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
              request.getActionName() == ActionMethodType.GetTabularMeta.name

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              val requestJsonObject = request.getRequestParameters()
              val transformOp: TransformOp = TransformOp.fromJsonObject(requestJsonObject)
              val dataFrame = transformOp.execute(new ExecutionContext {
                override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
                  Some(dataFrameHolder.work(new TaskRunner[DataFrameProviderService, DataFrame] {

                    override def acceptedBy(worker: DataFrameProviderService): Boolean = worker.accepts(dataFrameNameUrl)

                    override def executeWith(worker: DataFrameProviderService): DataFrame = worker.getDataFrame(dataFrameNameUrl, request.getUserPrincipal())

                    override def handleFailure(): DataFrame = throw new DataFrameNotFoundException(dataFrameNameUrl)
                  }))
                }
              })
              response.sendJsonObject(createDataFrameDescriber(dataFrame))
            }
          })

        case require: CollectGetStreamMethodEvent =>
          require.collect(
            new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean = {
                OpenedDataFrameRegistry.isTicketExists(request.getTicket)
              }

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                try {
                  val ticketId = request.getTicket
                  val dataFrame = OpenedDataFrameRegistry.getDataFrame(ticketId)
                  if(dataFrame.isEmpty) response.sendError(404, s"not found dataframe by ticket ${ticketId}")
                  else response.sendDataFrame(dataFrame.get)
                }catch {
                  case e: TicketNotFoundException =>
                    logger.error(e)
                    response.sendError(404, e.getMessage)
                  case e: TicketExpiryException =>
                    logger.error(e)
                    response.sendError(403, e.getMessage)
                  case e: Exception =>
                    logger.error(e)
                    response.sendError(500, e.getMessage)
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

