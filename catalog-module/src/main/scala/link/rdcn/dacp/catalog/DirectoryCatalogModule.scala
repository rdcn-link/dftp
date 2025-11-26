package link.rdcn.dacp.catalog

import link.rdcn.client.UrlValidator
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}
import link.rdcn.struct.ValueType.RefType
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, DataStreamSource, StructType, ValueType}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.vocabulary.RDF

import java.io.File

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/5 18:23
 * @Modified By:
 */
class DirectoryCatalogModule extends DftpModule {
  private var defaultRootDirectory: File = new File("data")
  private var dataSetName = defaultRootDirectory.getName

  def setDataSetName(dsName: String) = dataSetName = dsName

  def setRootDirectory(file: File): Unit = defaultRootDirectory = file

  private def isInDataDirectory(path: String): Boolean = new File(defaultRootDirectory, path).exists()

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireCatalogServiceEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireCatalogServiceEvent => r.holder.add(new CatalogService {
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
  }

  override def destroy(): Unit = {}
}
