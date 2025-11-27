package link.rdcn.server

import link.rdcn.client.UrlValidator
import link.rdcn.dacp.catalog.{CatalogService, CatalogServiceRequest, CollectCatalogServiceEvent}
import link.rdcn.server.module.{CollectDataFrameProviderEvent, DataFrameProviderService}
import link.rdcn.struct.ValueType.RefType
import link.rdcn.struct._
import link.rdcn.user.UserPrincipal
import link.rdcn.util.DataUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF

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
  private var dataSetName = defaultRootDirectory.getName

  def setDataSetName(dsName: String) = dataSetName = dsName

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
        event.isInstanceOf[CollectDataFrameProviderEvent] ||
          event.isInstanceOf[CollectCatalogServiceEvent]

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
          case r: CollectCatalogServiceEvent => r.holder.add(new CatalogService {
            override def accepts(request: CatalogServiceRequest): Boolean =
              request.getDataSetId == dataSetName || {
                val path = UrlValidator.extractPath(request.getDataFrameUrl)
                isInDataDirectory(path)
              }

            override def listDataSetNames(): List[String] = List(dataSetName)

            override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
              val model = ModelFactory.createDefaultModel()
              val NS = s"${serverContext.baseUrl}dataset#"

              val Dataset = model.createResource(NS + "Dataset")

              val name = model.createProperty(NS, "name")
              val path = model.createProperty(NS, "path")

              model.createResource(NS + "dataSetName")
                .addProperty(RDF.`type`, Dataset)
                .addProperty(name, dataSetName)
                .addProperty(path, defaultRootDirectory.getAbsolutePath)
            }

            override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
              val model = ModelFactory.createDefaultModel()
              val NS = s"${serverContext.baseUrl}dataFrame#"

              val df = model.createResource(NS + "DataFrame")

              val name = model.createProperty(NS, "name")
              val path = model.createProperty(NS, "path")

              model.createResource(NS + "dataSetName")
                .addProperty(RDF.`type`, df)
                .addProperty(name, dataFrameName)
                .addProperty(path, UrlValidator.extractPath(dataFrameName))
            }

            override def listDataFrameNames(dataSetId: String): List[String] = {
              defaultRootDirectory.listFiles().map("/" + _.getName).toList
            }

            override def getDocument(dataFrameName: String): DataFrameDocument = DataFrameDocument.empty()

            override def getStatistics(dataFrameName: String): DataFrameStatistics = new DataFrameStatistics {
              override def rowCount: Long = -1L

              override def byteSize: Long = new File(defaultRootDirectory, UrlValidator.extractPath(dataFrameName)).length()
            }

            override def getSchema(dataFrameName: String): Option[StructType] = {
              val file = new File(defaultRootDirectory, UrlValidator.extractPath(dataFrameName))
              if(file.isDirectory) Some(StructType.binaryStructType.add("url", RefType))
              else if(file.getName.endsWith(".csv")) Some(DataStreamSource.csv(file).schema)
              else if(file.getName.endsWith(".xlsx") || file.getName.endsWith(".xls"))
                Some(DataStreamSource.excel(file.getAbsolutePath).schema)
              else Some(StructType.empty.add("_1", ValueType.BinaryType))
            }

            override def getDataFrameTitle(dataFrameName: String): Option[String] =
              Some(UrlValidator.extractPath(dataFrameName))
          })
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}

}
