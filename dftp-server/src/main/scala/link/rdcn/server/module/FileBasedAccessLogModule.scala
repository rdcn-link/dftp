package link.rdcn.server.module

import link.rdcn.server.{Anchor, DftpModule, DftpRequest, DftpResponse, LogService, ServerContext}

class FileBasedAccessLogModule extends DftpModule {
  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    //read config file, get log file path...
    anchor.hook(new LogService {
      override def accepts(request: DftpRequest): Boolean = ???

      override def doLog(request: DftpRequest, response: DftpResponse): Unit = ???
    })
  }

  override def destroy(): Unit = ???
}
