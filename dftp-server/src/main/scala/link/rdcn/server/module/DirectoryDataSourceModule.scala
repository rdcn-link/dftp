package link.rdcn.server.module

import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventSourceService, ServerContext}
import link.rdcn.struct.ValueType.RefType
import link.rdcn.struct.{Blob, DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row, StructType}
import link.rdcn.util.{CodecUtils, DataUtils}
import org.json.JSONObject

import java.io.File
import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 09:41
 * @Modified By:
 */
class DirectoryDataSourceModule extends DftpModule{

  private def createBaseDftpDataSource(ctx: ServerContext): EventSourceService = {
    new EventSourceService {
      override def create(): CrossModuleEvent = new BaseDftpDataSource {
        val baseUrl = s"${ctx.getProtocolScheme()}://${ctx.getHost()}:${ctx.getPort()}/"
        override def getDataFrame(dataFrameUrl: String): DataFrame = {
          val dataFramePath = dataFrameUrl.stripPrefix(baseUrl).stripPrefix("/")
          val dfFile = if(ctx.getDftpHome().isEmpty) {
            Paths.get(ctx.getDftpHome().get, "data", dataFramePath).toFile
          }else Paths.get("data" + dataFramePath).toFile
          if(dfFile.isFile){
            dfFile.getName match {
              case fileName if(fileName.endsWith(".csv")) =>
                val ds = DataStreamSource.csv(dfFile)
                DefaultDataFrame(ds.schema, ds.iterator)
              case fileName if(fileName.endsWith(".xlsx") ||
                fileName.endsWith(".xls")) =>
                val ds = DataStreamSource.excel(dfFile.getAbsolutePath)
                DefaultDataFrame(ds.schema, ds.iterator)
              case _ => DataFrame.fromSeq(Seq(Blob.fromFile(dfFile)))
            }
          }else{
            val stream = DataUtils.listFilesWithAttributes(dfFile).toIterator
              .map(file => {
                if (file._1.isDirectory) {
                  (file._1.getName, -1L, "directory",
                    file._2.creationTime().toMillis,
                    file._2.lastModifiedTime().toMillis,
                    file._2.lastAccessTime().toMillis,
                    null,
                    DFRef((dataFrameUrl.stripSuffix("/") + File.separator + file._1.getName))
                  )
                }else{
                  (
                    file._1.getName, file._2.size(),
                    DataUtils.getFileType(file._1),
                    file._2.creationTime().toMillis,
                    file._2.lastModifiedTime().toMillis,
                    file._2.lastAccessTime().toMillis,
                    Blob.fromFile(file._1),
                    DFRef((dataFrameUrl.stripSuffix("/") + File.separator + file._1.getName))
                  )
                }
              }).map(Row.fromTuple(_))
            val schema = StructType.binaryStructType.add("url", RefType)
            DefaultDataFrame(schema, stream)
          }
        }

        override def action(actionName: String, parameter: Array[Byte]): Array[Byte] = {
          CodecUtils.encodeString(new JSONObject().put(actionName,"not implement").toString())
        }

        override def put(dataFrame: DataFrame): Array[Byte] = {
          CodecUtils.encodeString(new JSONObject().put("put","not implement").toString())
        }
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(createBaseDftpDataSource(serverContext))

  override def destroy(): Unit = {}

}
