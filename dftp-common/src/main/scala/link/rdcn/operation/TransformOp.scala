package link.rdcn.operation

import jep.{Jep, SharedInterpreter}
import link.rdcn.struct.{DataFrame, Row}
import link.rdcn.user.UserPrincipal
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.mutable.ListBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/1 17:02
 * @Modified By:
 */
trait ExecutionContext {
  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]

  def getSharedInterpreter(): Option[SharedInterpreter] = Some(SharedInterpreterManager.getInterpreter)
}

trait TransformOp {

  var inputs: Seq[TransformOp]

  def setInputs(operations: TransformOp*): TransformOp = {
    inputs = operations
    this
  }

  def sourceUrlList: Set[String] = inputs.flatMap(_.sourceUrlList).toSet

  def operationType: String

  def toJson: JSONObject

  def toJsonString: String = toJson.toString

  def execute(ctx: ExecutionContext): DataFrame
}

object TransformOp {
  def fromJsonString(json: String): TransformOp = {
    val parsed: JSONObject = new JSONObject(json)
    val opType = parsed.getString("type")
    if (opType == "SourceOp") {
      SourceOp(parsed.getString("dataFrameName"))
    } else {
      val ja: JSONArray = parsed.getJSONArray("input")
      val inputs = (0 until ja.length).map(ja.getJSONObject(_).toString()).map(fromJsonString(_))
      opType match {
        case "Map" => MapOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Filter" => FilterOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Limit" => LimitOp(parsed.getJSONArray("args").getInt(0), inputs: _*)
        case "Select" => SelectOp(inputs.head, parsed.getJSONArray("args").toList.asScala.map(_.toString): _*)
      }
    }
  }
}

case class SourceOp(dataFrameUrl: String) extends TransformOp {

  override var inputs: Seq[TransformOp] = Seq.empty

  override def operationType: String = "SourceOp"

  override def sourceUrlList: Set[String] = Set(dataFrameUrl)

  override def toJson: JSONObject = new JSONObject().put("type", operationType).put("dataFrameName", dataFrameUrl)

  override def execute(ctx: ExecutionContext): DataFrame = ctx.loadSourceDataFrame(dataFrameUrl)
    .getOrElse(throw new Exception(s"dataFrame $dataFrameUrl not found"))
}

case class MapOp(functionWrapper: FunctionWrapper, inputOperations: TransformOp*) extends TransformOp {

  override var inputs = inputOperations

  override def operationType: String = "Map"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
      .put("function", functionWrapper.toJson)
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val in = inputs.head.execute(ctx)
    in.map(functionWrapper.applyToInput(_, ctx).asInstanceOf[Row])
  }
}

case class FilterOp(functionWrapper: FunctionWrapper, inputOperations: TransformOp*) extends TransformOp {

  override var inputs = inputOperations

  override def operationType: String = "Filter"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
      .put("function", functionWrapper.toJson)
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val in = inputs.head.execute(ctx)
    in.filter(functionWrapper.applyToInput(_, ctx).asInstanceOf[Boolean])
  }
}

case class LimitOp(n: Int, inputOperations: TransformOp*) extends TransformOp {

  override var inputs = inputOperations

  override def operationType: String = "Limit"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
      .put("args", new JSONArray(Seq(n).asJava))
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val in = inputs.head.execute(ctx)
    in.limit(n)
  }
}

case class SelectOp(input: TransformOp, columns: String*) extends TransformOp {

  override var inputs: Seq[TransformOp] = Seq(input)

  override def operationType: String = "Select"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
      .put("args", new JSONArray(columns.asJava))
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val in = input.execute(ctx)
    in.select(columns: _*)
  }
}