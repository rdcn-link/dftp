package link.rdcn.dacp.recipe

import link.rdcn.dacp.optree.fifo.DockerContainer
import link.rdcn.dacp.optree.fifo.FileType.FileType
import link.rdcn.struct.DataFrame

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/12 21:07
 * @Modified By:
 */
trait FlowNode

trait Transformer11 extends FlowNode with Serializable {
  def transform(dataFrame: DataFrame): DataFrame
}

trait Transformer21 extends FlowNode with Serializable {
  def transform(leftDataFrame: DataFrame, rightDataFrame: DataFrame): DataFrame
}

case class RepositoryNode(
                           functionName: String,
                           functionVersion: Option[String],
                           args: Map[String, String] = Map.empty
                         ) extends FlowNode

case class RemoteDataFrameFlowNode(
                                    baseUrl: String,
                                    flow: Flow,
                                    certificate: String
                                  ) extends FlowNode

case class FifoFileBundleFlowNode(
                                   command: Seq[String],
                                   inputFilePath: Seq[(String, FileType)],
                                   outputFilePath: Seq[(String, FileType)],
                                   dockerContainer: DockerContainer
                                 ) extends FlowNode

case class FifoFileFlowNode() extends FlowNode

//只为DAG执行提供dataFrameName
case class SourceNode(dataFrameName: String) extends FlowNode

object FlowNode {
  def source(dataFrameName: String): SourceNode = {
    SourceNode(dataFrameName)
  }

  def ofTransformer11(transformer11: Transformer11): Transformer11 = {
    transformer11
  }

  def ofTransformer21(transformer21: Transformer21): Transformer21 = {
    transformer21
  }

  def ofScalaFunction(func: DataFrame => DataFrame): Transformer11 = {
    (dataFrame: DataFrame) => func(dataFrame)
  }

  def stocked(functionId: String, functionVersion: Option[String], args: Map[String, String] = Map.empty): RepositoryNode = {
    RepositoryNode(functionId, functionVersion, args)
  }

}