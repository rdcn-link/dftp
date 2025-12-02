/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/25 16:54
 * @Modified By:
 */
package link.rdcn.dacp.recipe

import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}

object FlowBuilder {
  /**
   * 将 org.json.JSONObject 的 properties 转换为 Map[String, String]
   */
  private def jsonObjectToMap(jsonObject: JSONObject): Map[String, String] = {
    jsonObject.keys().asScala.collect {
      case key: String =>
        // 确保所有值都被转换为 String
        key -> jsonObject.get(key).toString
    }.toMap
  }

  /**
   * 将 Stop 对象（JSONObject 形式）转换为具体的 FlowNode 对象
   */
  def stopToFlowNode(stopJson: JSONObject): FlowNode = {
    val instanceLabel = stopJson.getString("name")
    val nodeType = stopJson.getString("type")

    // 检查 properties 是否存在，如果没有则返回一个空的 JSONObject
    val properties = stopJson.optJSONObject("properties", new JSONObject())

    // 尝试从 properties 中获取算子的逻辑名称/ID。如果不存在，使用外部 name 作为备用。
    val operatorLogicName = properties.optString("name", instanceLabel)

    // 将所有 properties 转换为 Map[String, String]，用于 RepositoryNode 的 args
    val stringProps = jsonObjectToMap(properties)


    nodeType match {
      case "SourceNode" =>
        // 映射到 properties.name (即 operatorLogicName)
        SourceNode(dataFrameName = stringProps.getOrElse("path",""))

      case "RepositoryNode" =>
        // 映射到 properties.name (即 operatorLogicName) 作为 functionId
        RepositoryNode(
          operatorLogicName,
          stringProps.get("version"),
          stringProps // 包含 version 等其他属性
        )

      case other => throw new IllegalArgumentException(s"Unknown FlowNode type: $other")
    }
  }

  /**
   * 将 Source JSON 字符串转换为目标 Flow 中间类型
   */
  def buildFlow(sourceJsonString: String): Flow = {
    val root = new JSONObject(sourceJsonString)
    val flow = root.getJSONObject("flow")
    val stopsArray = flow.getJSONArray("stops")
    val pathsArray = flow.getJSONArray("paths")

    // 构建 Nodes Map (String -> FlowNode)
    val nodesMap: MMap[String, FlowNode] = MMap.empty

    for (i <- 0 until stopsArray.length()) {
      val stopJson = stopsArray.getJSONObject(i)
      val name = stopJson.getString("name")
      nodesMap(name) = stopToFlowNode(stopJson)
    }

    // 构建 Edges Map (String -> Seq[String])
    val edgesMap: MMap[String, Seq[String]] = MMap.empty

    for (i <- 0 until pathsArray.length()) {
      val pathJson = pathsArray.getJSONObject(i)
      val from = pathJson.getString("from")
      val to = pathJson.getString("to")

      val currentTargets = edgesMap.getOrElse(from, Seq.empty[String])
      edgesMap(from) = currentTargets :+ to
    }

    Flow(
      nodes = nodesMap.toMap,
      edges = edgesMap.toMap
    )
  }

  /**
   * 完整的转换流程
   */
  def convert(sourceJsonString: String): Either[String, Flow] = {
    try {
      Right(buildFlow(sourceJsonString))
    } catch {
      case e: Exception =>
        Left(s"JSON error: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }
}