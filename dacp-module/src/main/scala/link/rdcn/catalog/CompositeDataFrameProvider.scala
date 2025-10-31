package link.rdcn.catalog

import link.rdcn.message.{DataProviderRequest}
import link.rdcn.server.module.DataFrameProviderRequest
import link.rdcn.struct.{DataFrame, DataFrameDocument, DataFrameStatistics, DataStreamSource, StructType}
import org.apache.jena.rdf.model.Model

import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 19:14
 * @Modified By:
 */
class CompositeDataProvider extends DataProvider{
  private val datProviders = new ArrayBuffer[DataProvider]

  override def accepts(request: DataFrameProviderRequest): Boolean =
    datProviders.exists(_.accepts(request))

  def add(dataProvider: DataProvider) = datProviders.append(dataProvider)

  override def listDataSetNames(): List[String] =
    datProviders.flatMap(_.listDataSetNames()).toList

  override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = dataSetId

      override def getDataFrameUrl: String = null
    }
    datProviders.find(_.accepts(request)).foreach(_.getDataSetMetaData(dataSetId, rdfModel))
  }

  override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = null

      override def getDataFrameUrl: String = dataFrameName
    }
    datProviders.find(_.accepts(request)).foreach(_.getDataFrameMetaData(dataFrameName, rdfModel))
  }

  override def listDataFrameNames(dataSetId: String): List[String] = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = dataSetId

      override def getDataFrameUrl: String = null
    }
    datProviders.find(_.accepts(request))
      .map(_.listDataFrameNames(dataSetId)).getOrElse(List.empty)
  }

  override def getDataStreamSource(dataFrameName: String): DataStreamSource = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = null

      override def getDataFrameUrl: String = dataFrameName
    }
    datProviders.find(_.accepts(request))
      .map(_.getDataStreamSource(dataFrameName)).getOrElse(null)
  }

  override def getDocument(dataFrameName: String): DataFrameDocument = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = null

      override def getDataFrameUrl: String = dataFrameName
    }
    datProviders.find(_.accepts(request)).map(_.getDocument(dataFrameName))
      .getOrElse(DataFrameDocument.empty())
  }

  override def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = null

      override def getDataFrameUrl: String = dataFrameName
    }
    datProviders.find(_.accepts(request)).map(_.getStatistics(dataFrameName))
      .getOrElse(null)
  }

  override def getSchema(dataFrameName: String): Option[StructType] = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = null

      override def getDataFrameUrl: String = dataFrameName
    }
    datProviders.find(_.accepts(request)).map(_.getSchema(dataFrameName))
      .getOrElse(None)
  }

  override def getDataFrameTitle(dataFrameName: String): Option[String] = {
    val request = new DataProviderRequest {
      override def getDataSetId: String = null

      override def getDataFrameUrl: String = dataFrameName
    }
    datProviders.find(_.accepts(request)).map(_.getDataFrameTitle(dataFrameName))
      .getOrElse(None)
  }
}

