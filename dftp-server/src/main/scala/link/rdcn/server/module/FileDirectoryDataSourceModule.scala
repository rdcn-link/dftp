package link.rdcn.server.module

import link.rdcn.client.UrlValidator
import link.rdcn.server._
import link.rdcn.server.exception.DataFrameNotFoundException
import link.rdcn.struct.ValueType.RefType
import link.rdcn.struct.{Blob, DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row, StructType}
import link.rdcn.user.UserPrincipal
import link.rdcn.util.DataUtils

import java.io.File
import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/28 09:41
 * @Modified By:
 */
class FileDirectoryDataSourceModule extends DftpModule {

  private var defaultRootDirectory: File = new File("data")

  def setRootDirectory(file: File): Unit = defaultRootDirectory = file

  private def getDataFrameByUrl(dataFrameUrl: String, ctx: ServerContext): DataFrame = {
    val dataFramePath = dataFrameUrl.stripPrefix(ctx.baseUrl).stripPrefix("/")
    val dfFile = Paths.get(defaultRootDirectory.getAbsolutePath, dataFramePath).toFile

    if (dfFile.isFile) {
      dfFile.getName match {
        case fileName if (fileName.endsWith(".csv")) =>
          val ds = DataStreamSource.csv(dfFile)
          DefaultDataFrame(ds.schema, ds.iterator)
        case fileName if (fileName.endsWith(".xlsx") ||
          fileName.endsWith(".xls")) =>
          val ds = DataStreamSource.excel(dfFile.getAbsolutePath)
          DefaultDataFrame(ds.schema, ds.iterator)
        case _ => DataFrame.fromSeq(Seq(Blob.fromFile(dfFile)))
      }
    } else {
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
          } else {
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

  private def isInDataDirectory(path: String): Boolean = new File(defaultRootDirectory, path).exists()

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: CollectDataFrameProviderEvent =>
            require.holder.add(
              new DataFrameProviderService {
                override def getDataFrame(dataFrameUrl: String, user: UserPrincipal)(implicit ctx: ServerContext): DataFrame = {
                  getDataFrameByUrl(dataFrameUrl, ctx)
                }

                override def accepts(dataFrameUrl: String): Boolean = {
                  isInDataDirectory(UrlValidator.extractPath(dataFrameUrl))
                }
              })
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}

}
