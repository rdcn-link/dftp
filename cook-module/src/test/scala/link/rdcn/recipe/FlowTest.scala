/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:01
 * @Modified By:
 */
package link.rdcn.recipe

import link.rdcn.OtherNode
import link.rdcn.dacp.recipe.{FifoFileBundleFlowNode, Flow, FlowNode, FlowPath, SourceNode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class FlowTest {

  // 准备用于测试的模拟节点
  val nodeA: FlowNode = SourceNode("/path/A")
  val nodeB: FlowNode = OtherNode("B")
  val nodeC: FlowNode = OtherNode("C")
  val nodeD: FlowNode = OtherNode("D")
  val nodeE: FlowNode = FifoFileBundleFlowNode(null, null, null, null) // 类型合法即可

  /**
   * 测试 Flow.pipe() 工厂方法
   */
  @Test
  def testFlowPipe_HappyPath(): Unit = {
    val flow = Flow.pipe(nodeA, nodeB, nodeC)

    // 1. 验证节点
    val expectedNodes = Map(
      "0" -> nodeA,
      "1" -> nodeB,
      "2" -> nodeC
    )
    assertEquals(expectedNodes, flow.nodes, "Flow.pipe() 创建的节点 Map 不匹配")

    // 2. 验证边
    val expectedEdges = Map(
      "0" -> Seq("1"),
      "1" -> Seq("2")
    )
    assertEquals(expectedEdges, flow.edges, "Flow.pipe() 创建的边 Map 不匹配")
  }

  /**
   * 测试 Flow.pipe() 只有一个节点（边缘情况）
   */
  @Test
  def testFlowPipe_SingleNode(): Unit = {
    val flow = Flow.pipe(nodeA)

    assertEquals(Map("0" -> nodeA), flow.nodes, "单节点 Flow 的 nodes 不匹配")
    assertTrue(flow.edges.isEmpty, "单节点 Flow 的 edges 应为空")
  }

  /**
   * 测试 Flow.pipe() 没有节点（失败情况）
   */
  @Test
  def testEmptyFlow_Fails(): Unit = {
    val emptyFlow = Flow(Map.empty, Map.empty)

    // 遵守 [2025-09-26] 规范：(expected, actual, message)
    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      emptyFlow.getExecutionPaths() // 验证 getExecutionPaths 对空图的处理
      ()
    }, "空图应在 getExecutionPaths 中抛出异常")

    assertTrue(ex.getMessage.contains("Cycle detected"), "异常消息应指明检测到循环（或无根节点）")
  }
  /**
   * 测试 getExecutionPaths() - 线性图
   * (使用 Flow.pipe() 创建的图)
   */
  @Test
  def testGetExecutionPaths_Linear(): Unit = {
    val flow = Flow.pipe(nodeA, nodeB, nodeC)

    // 执行
    val paths = flow.getExecutionPaths()

    // 验证 (A -> B -> C)
    // getExecutionPaths 找到 Sink (C)，然后向后构建
    // C(B(A))
    val expectedPath = FlowPath(nodeC, Seq(
      FlowPath(nodeB, Seq(
        FlowPath(nodeA, Seq.empty)
      ))
    ))

    assertEquals(1, paths.length, "线性图应只有 1 条执行路径")
    assertEquals(expectedPath, paths.head, "线性图的执行路径构建不正确")
  }

  /**
   * 测试 getExecutionPaths() - 分支图
   * A -> B
   * A -> C
   */
  @Test
  def testGetExecutionPaths_Branching(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB, "C" -> nodeC)
    val edges = Map("A" -> Seq("B", "C"))
    val flow = Flow(nodes, edges)

    // 执行
    val paths = flow.getExecutionPaths()

    // 验证
    // Sinks (根节点) 是 B 和 C
    // 路径 1: C(A)
    val expectedPathC = FlowPath(nodeC, Seq(FlowPath(nodeA, Seq.empty)))
    // 路径 2: B(A)
    val expectedPathB = FlowPath(nodeB, Seq(FlowPath(nodeA, Seq.empty)))

    assertEquals(2, paths.length, "分支图应有 2 条执行路径")
    // 使用 Set 比较，因为 Sinks (B, C) 的顺序是不确定的
    assertEquals(Set(expectedPathC, expectedPathB), paths.toSet, "分支图的执行路径构建不正确")
  }

  /**
   * 测试 getExecutionPaths() - 聚合图
   * A -> C
   * B -> C
   */
  @Test
  def testGetExecutionPaths_Merging(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeE, "C" -> nodeC) // B 是 FifoFileBundleFlowNode (合法根节点)
    val edges = Map("A" -> Seq("C"), "B" -> Seq("C"))
    val flow = Flow(nodes, edges)

    // 执行
    val paths = flow.getExecutionPaths()

    // 验证
    // Sink (根节点) 是 C
    // 路径: C(A, B)
    val expectedPath = FlowPath(nodeC, Seq(
      FlowPath(nodeA, Seq.empty),
      FlowPath(nodeE, Seq.empty)
    ))

    assertEquals(1, paths.length, "聚合图应有 1 条执行路径")
    // 比较子节点时使用 Set，因为 (A, B) 的顺序是不确定的
    assertEquals(expectedPath.node, paths.head.node, "聚合图的根节点 (Sink) 不匹配")
    assertEquals(expectedPath.children.toSet, paths.head.children.toSet, "聚合图的子路径（父节点）不匹配")
  }

  /**
   * 测试 getExecutionPaths() - 完整 DAG
   * A -> B -> D
   * E -> B
   * A -> C
   */
  @Test
  def testGetExecutionPaths_ComplexDAG(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB, "C" -> nodeC, "D" -> nodeD, "E" -> nodeE)
    val edges = Map(
      "A" -> Seq("B", "C"),
      "B" -> Seq("D"),
      "E" -> Seq("B")
    )
    val flow = Flow(nodes, edges)

    // 执行
    val paths = flow.getExecutionPaths()

    // 验证
    // Sinks (根节点) 是 C 和 D
    // 路径 1 (从 C): C(A)
    val expectedPathC = FlowPath(nodeC, Seq(FlowPath(nodeA, Seq.empty)))

    // 路径 2 (从 D): D(B(A, E))
    val expectedPathD = FlowPath(nodeD, Seq(
      FlowPath(nodeB, Seq(
        FlowPath(nodeA, Seq.empty),
        FlowPath(nodeE, Seq.empty)
      ))
    ))

    assertEquals(2, paths.length, "复杂 DAG 应有 2 条执行路径")

    // 查找并验证 C 路径
    val pathC = paths.find(_.node == nodeC).get
    assertEquals(expectedPathC, pathC, "路径 C(A) 构建不正确")

    // 查找并验证 D 路径
    val pathD = paths.find(_.node == nodeD).get
    assertEquals(expectedPathD.node, pathD.node, "路径 D 的根节点不匹配")
    assertEquals(1, pathD.children.length, "路径 D 应有 1 个子路径 (B)")
    val pathB = pathD.children.head
    assertEquals(nodeB, pathB.node, "路径 D 的子节点 (B) 不匹配")
    // 验证 B 的父节点 (A, E)
    assertEquals(Set(nodeA, nodeE), pathB.children.map(_.node).toSet, "节点 B 的父节点 (A, E) 不匹配")
  }

  /**
   * 测试 detectRootNodes() - 根节点类型非法
   */
  @Test
  def testRootNodeValidation_InvalidType_Fails(): Unit = {
    val invalidRootNode = OtherNode("InvalidRoot")
    val nodes = Map("A" -> invalidRootNode, "B" -> nodeB)
    val edges = Map("A" -> Seq("B")) // A 是根节点
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths() // detectRootNodes 在内部被调用
      ()
    }, "根节点不是 SourceNode 或 FifoFileBundleFlowNode 时应抛出异常")

    assertTrue(ex.getMessage.contains("is not of type SourceOp"), "异常消息应指明根节点类型错误")
  }

  /**
   * 测试 getExecutionPaths() - 节点未定义
   */
  @Test
  def testNodeValidation_NodeNotInMap_Fails(): Unit = {
    // C 在 edges 中被引用，但不在 nodes 中
    val nodes = Map("A" -> nodeA, "B" -> nodeB)
    val edges = Map("A" -> Seq("C"))
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths()
      ()
    }, "当节点在 edges 中但不在 nodes 中时应抛出异常")

    assertTrue(ex.getMessage.contains("node 'C' is not defined in the node map"), "异常消息应指明节点未定义")
  }

  /**
   * 测试 getExecutionPaths() - 检测到简单循环 (A -> B, B -> A)
   */
  @Test
  def testCycleDetection_SimpleLoop_Fails(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB)
    val edges = Map("A" -> Seq("B"), "B" -> Seq("A"))
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths() // 应该在 detectRootNodes 中失败
      ()
    }, "有循环的图应抛出异常")

    assertTrue(ex.getMessage.contains("no root nodes found"), "异常消息应指明没有根节点（因为有循环）")
  }

  /**
   * 测试 getExecutionPaths() - 检测到更复杂的循环
   * A -> B -> C -> B (在 buildPath 中检测到)
   */
  @Test
  def testCycleDetection_ComplexLoop_Fails(): Unit = {
    val nodes = Map("A" -> nodeA, "B" -> nodeB, "C" -> nodeC, "D" -> nodeD)
    val edges = Map(
      "A" -> Seq("B"),
      "B" -> Seq("C", "D"), // D 是 Sink
      "C" -> Seq("B") // 循环 C -> B
    )
    val flow = Flow(nodes, edges)

    val ex = assertThrows(classOf[IllegalArgumentException], () => {
      flow.getExecutionPaths() // 应该在 buildPath 中失败
      ()
    }, "有循环的图应抛出异常")

    assertTrue(ex.getMessage.contains("Cycle detected: node 'B' is revisited"), "异常消息应指明在 'B' 处检测到循环")
  }
}
