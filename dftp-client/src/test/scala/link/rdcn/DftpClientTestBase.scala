package link.rdcn

import link.rdcn.server.module.{CollectGetStreamMethodEvent, FilteredGetStreamMethods, GetStreamMethod, Workers}
import link.rdcn.server._
import link.rdcn.struct.ValueType.{IntType, StringType}
import link.rdcn.struct._

import java.io.File
import java.nio.file.{Files, Paths}


/** *
 * 所有测试用相关公共类和变量
 */
trait DftpClientTestBase {

}

object DftpClientTestBase {

  // 文件数量配置
  val binFileCount = 3
  val csvFileCount = 3

  val adminUsername = "admin@instdb.cn"
  val adminPassword = "admin001"

  val resourceUrl = getClass.getProtectionDomain.getCodeSource.getLocation
  val testClassesDir = new File(resourceUrl.toURI)

  def getOutputDir(subDirs: String*): String = {
    val outDir = Paths.get(testClassesDir.getParentFile.getParentFile.getAbsolutePath, subDirs: _*) // 项目根路径
    Files.createDirectories(outDir)
    outDir.toString
  }

  def getLine(row: Row): String = {
    val delimiter = ","
    row.toSeq.map(_.toString).mkString(delimiter) + '\n'
  }

}

class MockServerContext extends ServerContext {
  override def getHost(): String = "0.0.0.0"
  override def getPort(): Int = 3101
  override def getProtocolScheme(): String = "dftp"
  override def getDftpHome(): Option[String] = None
}


class GetStreamModule extends DftpModule {
  private val mockSchema: StructType = StructType.empty.add("id", IntType).add("name", StringType)
  private val getStreamHolder = new FilteredGetStreamMethods
  private var serverContext: ServerContext = _
  private val eventHandler = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean = true

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case r: CollectGetStreamMethodEvent => r.collect(
          new GetStreamMethod {
            override def accepts(request: DftpGetStreamRequest): Boolean = request.asInstanceOf[DftpGetPathStreamRequest].getRequestURL().contains("oldStream")

            override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
              request.asInstanceOf[DftpGetPathStreamRequest].getRequestURL() match {
                case url if url.contains("oldStream") =>
                  response.sendDataFrame(mockDF)
              }
            }
          })
        case _ =>
      }
    }

    def mockDF: DataFrame = DefaultDataFrame(
      mockSchema, Seq(Row(1, "data")).iterator
    )
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext
    anchor.hook(eventHandler)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(new CollectGetStreamMethodEvent(getStreamHolder))
    })
  }

  override def destroy(): Unit = {
  }
}

