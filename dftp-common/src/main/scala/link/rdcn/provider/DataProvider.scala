package link.rdcn.provider

import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, DataStreamSource, StructType}
import org.apache.jena.rdf.model.Model

import java.util.{List => JList}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:14
 * @Modified By:
 */
trait DataProvider {

  /**
   * 列出所有数据集名称
   *
   * @return java.util.List[String]
   */
  def listDataSetNames(): JList[String]

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
  def listDataFrameNames(dataSetId: String): JList[String]

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

  def getSchema(dataFrameName: String): StructType

  /** *
   * 获取数据帧统计信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameStatistics
   */
  def getStatistics(dataFrameName: String): DataFrameStatistics
}
