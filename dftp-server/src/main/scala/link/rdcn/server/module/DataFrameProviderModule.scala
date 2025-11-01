package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:43
 * @Modified By:
 */
class DataFrameProviderModule(dataFrameProvider: DataFrameProvider) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireDataFrameProviderEvent => r.add(dataFrameProvider)
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

trait DataFrameProviderRequest {
  def getDataFrameUrl: String
}

object DataFrameProviderRequest {
  def create(dataFrameUrl: String): DataFrameProviderRequest = {
    new DataFrameProviderRequest {
      override def getDataFrameUrl: String = dataFrameUrl
    }
  }
}

trait DataFrameProvider{
  def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame
  def accepts(request: DataFrameProviderRequest): Boolean
}

class CompositeDataFrameProvider extends DataFrameProvider{
  private val dataFrameProviders = new ArrayBuffer[DataFrameProvider]

  override def accepts(request: DataFrameProviderRequest): Boolean =
    dataFrameProviders.exists(_.accepts(request))

  override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame = {
    val request = DataFrameProviderRequest.create(dataFrameUrl)
    dataFrameProviders.find(_.accepts(request)).map(_.getDataFrame(dataFrameUrl, userPrincipal)).orNull
  }

  def add(dataFrameProvider: DataFrameProvider) = dataFrameProviders.append(dataFrameProvider)
}

case class RequireDataFrameProviderEvent(composite: CompositeDataFrameProvider) extends CrossModuleEvent{
  def add(dataFrameProvider: DataFrameProvider) = composite.add(dataFrameProvider)
}