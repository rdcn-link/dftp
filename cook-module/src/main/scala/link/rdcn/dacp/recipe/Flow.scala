package link.rdcn.dacp.recipe

import link.rdcn.dacp.optree.FileRepositoryBundle
import scala.annotation.varargs

/**
 * @Author renhao
 * @Description:
 * Flow(
 * Nodes(
 * "A" -> SourceOp("/abcd"),
 * "B" -> Functiion.STOCKED("cn.piflow.Transfomer1", params),
 * "C" -> Function.PYTHON_Code(code),
 * "D" -> Function.JAVA(bytes2)
 * ),
 * Edges(
 * "A" -> Seq("B", "C"),
 * "B" -> "D"
 * )
 * )
 * @Data 2025/7/12 21:30
 * @Modified By:
 */
case class FlowPath(node: FlowNode, children: Seq[FlowPath] = Seq.empty)

case class Flow(
                 nodes: Map[String, FlowNode],
                 edges: Map[String, Seq[String]]
               ) {
  /**
   * Edges(
   * "A" -> Seq("B", "C"),        A->B->D
   * ^
   * E
   * "B" -> "D"               =>  A->C
   * "E" -> "B"
   * )
   * )
   * */
  def getExecutionPaths(): Seq[FlowPath] = {
    detectRootNodes(edges)
    val reverseEdges = edges.toSeq
      .flatMap { case (parent, children) =>
        children.map(child => child -> parent)
      }
      .groupBy(_._1)
      .map { case (k, v) => k -> v.map(_._2) }
    val allTargets = reverseEdges.values.flatten.toSet
    val allSources = reverseEdges.keySet
    val rootNodes = if (allSources.nonEmpty) allSources.diff(allTargets) else nodes.keySet
    if(rootNodes.isEmpty) throw new IllegalArgumentException(s"Cycle detected")
    def buildPath(current: String, visited: Set[String]): FlowPath = {
      if (visited.contains(current)) {
        throw new IllegalArgumentException(s"Cycle detected: node '$current' is revisited")
      }
      val children = reverseEdges.getOrElse(current, Seq.empty)
      val currentNode = nodes.get(current).getOrElse(throw new IllegalArgumentException(
        s"Invalid DAG: node '$current' is not defined in the node map."
      ))
      FlowPath(currentNode, children.map(child => buildPath(child, visited + current)))
    }

    rootNodes.toSeq.map(root => buildPath(root, Set.empty))
  }

  private def detectRootNodes(edges: Map[String, Seq[String]]): Unit = {
    val allTargets = edges.values.flatten.toSet
    val allSources = edges.keySet
    val rootNodes = allSources.diff(allTargets) // 没有被其他节点指向的起始节点
    if (rootNodes.isEmpty && allSources.nonEmpty) throw new IllegalArgumentException("Invalid DAG: no root nodes found, graph might contain cycles or be empty")
    rootNodes.foreach(rootKey => nodes.get(rootKey) match {
      case Some(_: SourceNode) => // 合法，继续
      case Some(_: FifoFileBundleFlowNode) => //合法, 继续
      case Some(other) => throw new IllegalArgumentException(s"Invalid DAG: root node '$other' is not of type SourceOp, but ${other.getClass.getSimpleName}.")
      case None => throw new IllegalArgumentException(s"Invalid DAG: root node '$rootKey' is not defined in the node map.")
    })
  }
}

object Flow {
  @varargs
  def pipe(head: FlowNode, tail: FlowNode*): Flow = {
    val nodes = head +: tail
    if (nodes.isEmpty) {
      throw new IllegalArgumentException("one node at least")
    }
    val pairs = nodes.zipWithIndex.map {
      case (node, index) =>
        (index.toString, node)
    }.toMap
    val keysSeq = pairs.keys.toSeq
    val edges = keysSeq.zip(keysSeq.tail).map { case (currentElement, nextElement) =>
      currentElement -> Seq(nextElement)
    }.toMap
    Flow(pairs, edges)
  }
}

