package link.rdcn.server.module

import link.rdcn.server._

import java.io.File

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 09:41
 * @Modified By:
 */
class DirectoryDataSourceModule extends DftpModule {

  var _dir: File = null

  def setDir(dir: File): DirectoryDataSourceModule = {
    _dir = dir
    this
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequiresGetStreamHandler]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequiresGetStreamHandler =>
            require.add(new GetStreamHandler() {

              override def accepts(request: DftpGetStreamRequest): Boolean = ???

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = ???
            })
        }
      }
    })

  override def destroy(): Unit = {}

}
