package link.rdcn.catalog

import link.rdcn.catalog.CatalogFormatter._
import link.rdcn.client.UrlValidator
import link.rdcn.server.ServerContext
import link.rdcn.server.module.{DataFrameProvider, DataFrameProviderRequest}
import link.rdcn.struct.ValueType.{LongType, RefType, StringType}
import link.rdcn.struct._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.JSONObject

import java.io.StringWriter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:14
 * @Modified By:
 */
trait DataProvider extends DataFrameProvider {

  override def accepts(request: DataFrameProviderRequest): Boolean

  /**
   * 列出所有数据集名称
   *
   * @return java.util.List[String]
   */
  def listDataSetNames(): List[String]

  /**
   * 获取数据集的 RDF 元数据，填充到传入的 rdfModel 中
   *
   * @param dataSetId 数据集 ID
   * @param rdfModel  RDF 模型（由调用者传入，方法将其填充）
   */
  def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit

  /**
   * 获取数据集的 RDF 元数据，填充到传入的 rdfModel 中
   *
   * @param dataFrameName 数据帧名（如 /mnt/a.csv)
   * @param rdfModel  RDF 模型（由调用者传入，方法将其填充）
   */
  def  getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit

  /**
   * 列出指定数据集下的所有数据帧名称
   *
   * @param dataSetId 数据集 ID
   * @return java.util.List[String]
   */
  def listDataFrameNames(dataSetId: String): List[String]

  /**
   * 获取数据帧的数据流
   *
   * @param dataFrameName 数据帧名（如 /mnt/a.csv)
   * @return 数据流源
   */
  def getDataStreamSource(dataFrameName: String): DataStreamSource

  /**
   * 获取数据帧详细信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameDocument
   */
  def getDocument(dataFrameName: String): DataFrameDocument

  /** *
   * 获取数据帧统计信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameStatistics
   */
  def getStatistics(dataFrameName: String): DataFrameStatistics

  def getSchema(dataFrameName: String): Option[StructType]

  def getDataFrameTitle(dataFrameName: String): Option[String]

  final override def getDataFrame(dataFrameUrl: String)(implicit ctx: ServerContext): DataFrame = {
    UrlValidator.extractPath(dataFrameUrl) match {
      case "/listDataSets" => doListDataSets(ctx.baseUrl)
      case path if path.startsWith("/listDataFrames") =>
        doListDataFrames(path, ctx.baseUrl)
      case path if path.startsWith("/listHosts") =>
        doListHostInfo(ctx)
      case _ => getDataStreamSource(dataFrameUrl).dataFrame
    }
  }


  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataSets
   * 返回链接： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * */
  private def doListDataSets(baseUrl: String): DataFrame = {
    val stream = listDataSetNames().map(dsName => {
      val model: Model = ModelFactory.createDefaultModel
      getDataSetMetaData(dsName, model)
      val writer = new StringWriter();
      model.write(writer, "RDF/XML");
      val dataSetInfo = new JSONObject().put("name", dsName).toString
      Row.fromTuple((dsName, writer.toString
        , dataSetInfo, DFRef(s"${baseUrl}/listDataFrames/$dsName")))
    }).toIterator
    val schema = StructType.empty.add("name", StringType)
      .add("meta", StringType).add("DataSetInfo", StringType).add("dataFrames", RefType)
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * 返回链接： dacp://0.0.0.0:3101/dataFrameName
   * */
  private def doListDataFrames(listDataFrameUrl: String, baseUrl: String): DataFrame = {
    val dataSetName = listDataFrameUrl.stripPrefix("/listDataFrames/")
    val schema = StructType.empty.add("name", StringType)
      .add("size", LongType)
      .add("title", StringType)
      .add("document", StringType)
      .add("schema", StringType)
      .add("statistics", StringType)
      .add("dataFrame", RefType)
    val stream: Iterator[Row] = listDataFrameNames(dataSetName)
      .map(dfName => {
        val dfSchema = getSchema(dfName)
        (dfName,
          getStatistics(dfName).rowCount,
          getDataFrameTitle(dfName).getOrElse(null),
          getDataFrameDocumentJsonString(getDocument(dfName), dfSchema),
          schema.toString,
          getDataFrameStatisticsString(getStatistics(dfName)),
          DFRef(s"${baseUrl}/$dfName"))
      })
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接(实现链接)： dacp://0.0.0.0:3101/listHosts
   * */
  private def doListHostInfo(serverContext: ServerContext): DataFrame = {
    val schema = StructType.empty.add("hostInfo", StringType).add("resourceInfo", StringType)
    val stream = Seq((getHostInfoString(serverContext), getHostResourceString()))
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }
}