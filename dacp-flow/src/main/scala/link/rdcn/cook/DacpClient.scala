package link.rdcn.cook

import link.rdcn.client.{DftpClient, RemoteDataFrameProxy, UrlValidator}
import link.rdcn.operation.{DataFrameCall11, DataFrameCall21, SerializableFunction, SourceOp, TransformOp}
import link.rdcn.optree.{FiFoFileNode, FileRepositoryBundle, LangTypeV2, RepositoryOperator, TransformFunctionWrapper, TransformerNode}
import link.rdcn.recipe.{ExecutionResult, FifoFileBundleFlowNode, FifoFileFlowNode, Flow, FlowPath, RepositoryNode, SourceNode, Transformer11, Transformer21}
import link.rdcn.struct.{ClosableIterator, DataFrame, Row, StructType}
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}
import org.apache.arrow.flight.Ticket
import org.json.{JSONArray, JSONObject}

import java.io.File
import scala.collection.JavaConverters.asJavaCollectionConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 20:12
 * @Modified By:
 */
class DacpClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  def cook(recipe: Flow): ExecutionResult = {
    val executePaths: Seq[FlowPath] = recipe.getExecutionPaths()
    val dfs: Seq[DataFrame] = executePaths.map(path => RemoteDataFrameProxy(transformFlowToOperation(path), getCookRows))
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt - 1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  private def transformFlowToOperation(path: FlowPath): TransformOp = {
    path.node match {
      case f: Transformer11 =>
        val genericFunctionCall = DataFrameCall11(new SerializableFunction[DataFrame, DataFrame] {
          override def apply(v1: DataFrame): DataFrame = f.transform(v1)
        })
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall),
          transformFlowToOperation(path.children.head))
        transformerNode
      case f: Transformer21 =>
        val genericFunctionCall = DataFrameCall21(new SerializableFunction[(DataFrame, DataFrame), DataFrame] {
          override def apply(v1: (DataFrame, DataFrame)): DataFrame = f.transform(v1._1, v1._2)
        })
        val leftInput = transformFlowToOperation(path.children.head)
        val rightInput = transformFlowToOperation(path.children.last)
        val transformerNode: TransformerNode = TransformerNode(TransformFunctionWrapper.getJavaSerialized(genericFunctionCall), leftInput, rightInput)
        transformerNode
      case node: RepositoryNode =>
        val jo = new JSONObject()
        jo.put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
        jo.put("functionID", node.functionId)
        val transformerNode: TransformerNode = TransformerNode(
          TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[RepositoryOperator],
          transformFlowToOperation(path.children.head))
        transformerNode
      case FifoFileBundleFlowNode(command, inputFilePath, outputFilePath, dockerContainer) =>
        val jo = new JSONObject()
        jo.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
        jo.put("command", new JSONArray(command.asJavaCollection))
        jo.put("inputFilePath", new JSONArray(inputFilePath.asJavaCollection))
        jo.put("outputFilePath", new JSONArray(outputFilePath.asJavaCollection))
        jo.put("dockerContainer", dockerContainer.toJson())
        val transformerNode: TransformerNode = TransformerNode(
          TransformFunctionWrapper.fromJsonObject(jo).asInstanceOf[FileRepositoryBundle],
          path.children.map(transformFlowToOperation(_)): _* )
        transformerNode
      case FifoFileFlowNode(filePath) => FiFoFileNode(filePath, path.children.map(transformFlowToOperation(_)): _*)
      case s: SourceNode => SourceOp(s.dataFrameName)
      case other => throw new IllegalArgumentException(s"This FlowNode ${other} is not supported please extend Transformer11 trait")
    }
  }

  def getCookRows(transformOpStr: String): (StructType, ClosableIterator[Row]) = {
    val schemaAndIter = getStream(new Ticket(CookTicket(transformOpStr).encodeTicket()))
    val stream = schemaAndIter._2.map(seq => Row.fromSeq(seq))
    (schemaAndIter._1, ClosableIterator(stream)())
  }
}

object DacpClient {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS, useUnifiedLogin: Boolean = false): DacpClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101))
        if(useUnifiedLogin){
          credentials match {
            case AnonymousCredentials => client.login(credentials)
            case c: UsernamePassword => client.login(AuthPlatform.authenticate(c))
            case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
          }
        }else client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, file: File, credentials: Credentials = Credentials.ANONYMOUS, useUnifiedLogin: Boolean = false): DacpClient = {
    System.setProperty("javax.net.ssl.trustStore", file.getAbsolutePath)
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101), true)
        if(useUnifiedLogin){
          credentials match {
            case AnonymousCredentials => client.login(credentials)
            case c: UsernamePassword => client.login(AuthPlatform.authenticate(c))
            case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
          }
        }else client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
