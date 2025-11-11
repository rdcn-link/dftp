package link.rdcn.server.module

import link.rdcn.server._
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:43
 * @Modified By:
 */
class DataFrameProviderModule(dataFrameProvider: DataFrameProviderService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireDataFrameProviderEvent =>
            r.holder.set(old => {
              new DataFrameProviderService {
                override def accepts(dataFrameUrl: String): Boolean =
                  dataFrameProvider.accepts(dataFrameUrl) || old !=null && old.accepts(dataFrameUrl)

                override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                                         (implicit ctx: ServerContext): DataFrame = {
                  if(dataFrameProvider.accepts(dataFrameUrl))
                    dataFrameProvider.getDataFrame(dataFrameUrl, userPrincipal)
                  else if(old!=null && old.accepts(dataFrameUrl))
                    old.getDataFrame(dataFrameUrl, userPrincipal)
                  else throw new DataFrameNotFoundException(dataFrameUrl)
                }
              }
            })
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

trait DataFrameProviderService{
  def accepts(dataFrameUrl: String): Boolean

  def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame
}

case class RequireDataFrameProviderEvent(holder: ObjectHolder[DataFrameProviderService]) extends CrossModuleEvent