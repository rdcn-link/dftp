package link.rdcn.client

import link.rdcn.dacp.optree.{FiFoFileNode, FileRepositoryBundle, LangTypeV2, RepositoryOperator, TransformFunctionWrapper, TransformerNode}
import link.rdcn.dacp.recipe.{ExecutionResult, FifoFileBundleFlowNode, FifoFileFlowNode, Flow, FlowPath, RepositoryNode, SourceNode, Transformer11, Transformer21}
import link.rdcn.message.DftpTicket
import link.rdcn.operation.{DataFrameCall11, DataFrameCall21, SerializableFunction, SourceOp, TransformOp}
import link.rdcn.struct.{ClosableIterator, DFRef, DataFrame, DataFrameDocument, DataFrameStatistics, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}
import org.apache.arrow.flight.Ticket
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.{File, StringReader}
import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 20:12
 * @Modified By:
 */
class DacpClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  private val dacpUrlPrefix: String = s"dacp://$host:$port"

  def listDataSetNames(): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix + "/listDataSets").mapIterator(rows => rows.foreach(row => {
      result+=(row.getAs[String](0))
    }))
    result
  }

  def listDataFrameNames(dsName: String): Seq[String] = {
    val result = ArrayBuffer[String]()
    get(dacpUrlPrefix+ s"/listDataFrames/$dsName")
      .foreach(row => result.append(row.getAs[String](0)))
    result
  }

  def getDataSetMetaData(dsName: String): Model = {
    val rdfString = new String(doAction(s"/getDataSetMetaData/${dsName}"), "UTF-8").trim
    getModelByString(rdfString)
  }

  def getDataFrameMetaData(dfName: String): Model = {
    val rdfString = new String(doAction(s"/getDataFrameMetaData/${dfName}"), "UTF-8").trim
    getModelByString(rdfString)
  }

  private def getModelByString(rdfString: String): Model = {
    val model = ModelFactory.createDefaultModel()
    rdfString match {
      case s if s.nonEmpty =>
        val reader = new StringReader(s)
        model.read(reader, null, "RDF/XML")
      case _ =>
    }
    model
  }

  def getSchema(dataFrameName: String): StructType = {
    val structTypeStr = new String(doAction(s"/getSchema/${dataFrameName}"), "UTF-8")
    StructType.fromString(structTypeStr)
  }

  def getDataFrameTitle(dataFrameName: String): String = {
    new String(doAction(s"/getDataFrameTitle/${dataFrameName}"), "UTF-8")
  }

  def getDocument(dataFrameName: String): DataFrameDocument = {

    new String(doAction(s"/getDocument/${dataFrameName}"), "UTF-8").trim match {
      case s if s.nonEmpty =>
        val jo = new JSONArray(s).getJSONObject(0)
        new DataFrameDocument {
          override def getSchemaURL(): Option[String] = Some("SchemaUrl")

          override def getDataFrameTitle(): Option[String] = Some(jo.getString("DataFrameTitle"))

          override def getColumnURL(colName: String): Option[String] = Some(jo.getString("ColumnUrl"))

          override def getColumnAlias(colName: String): Option[String] = Some(jo.getString("ColumnAlias"))

          override def getColumnTitle(colName: String): Option[String] = Some(jo.getString("ColumnTitle"))
        }
      case _ => DataFrameDocument.empty()
    }

  }

  def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val jsonString: String = {
      new String(doAction(s"/getStatistics/${dataFrameName}"), "UTF-8").trim match {
        case s if s.isEmpty => ""
        case s => s
      }
    }
    val jo = new JSONObject(jsonString)
    new DataFrameStatistics {
      override def rowCount: Long = jo.getLong("rowCount")

      override def byteSize: Long = jo.getLong("byteSize")
    }
  }

  def getDataFrameSize(dataFrameName: String): Long = {
    new String(doAction(s"/getDataFrameSize/${dataFrameName}"), "UTF-8").trim match {
      case s if s.nonEmpty => s.toLong
      case _ => 0L
    }
  }

  def getHostInfo: Map[String, String] = {
    val result = mutable.Map[String, String]()
    get(dacpUrlPrefix + "/listHostInfo").mapIterator(iter => iter.foreach(row => {
      result.put(row.getAs[String](0), row.getAs[String](1))
    }))
    val jo = new JSONObject(result.toMap.head._2)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val result = mutable.Map[String, String]()
    get(dacpUrlPrefix + "/listHostInfo").mapIterator(iter => iter.foreach(row => {
      result.put(row.getAs[String](0), row.getAs[String](2))
    }))
    val jo = new JSONObject(result.toMap.head._2)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def executeTransformTree(transformOp: TransformOp): DataFrame = {
    val schemaAndRow = getCookRows(transformOp.toJsonString)
    DefaultDataFrame(schemaAndRow._1, schemaAndRow._2)
  }

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
  private case class CookTicket(ticketContent: String) extends DftpTicket {
    override val typeId: Byte = 3
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
            case c: UsernamePassword => client.login(OdcAuthClient.requestAccessToken(c))
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
            case c: UsernamePassword => client.login(OdcAuthClient.requestAccessToken(c))
            case _ => throw new IllegalArgumentException(s"the $credentials is not supported")
          }
        }else client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
