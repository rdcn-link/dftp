/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:10
 * @Modified By:
 */
package link.rdcn.optree.fifo

import link.rdcn.dacp.optree.fifo.{DockerContainer, DockerExecute}
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.{Disabled, Test}

import java.io.File
import java.util.UUID

class DockerContainerTest {

  /**
   * 测试 JSON 序列化 (toJson) 和反序列化 (fromJson) 的往返能力
   * 场景：所有字段均有值
   */
  @Test
  def testJsonSerializationRoundTrip_Full(): Unit = {
    val originalContainer = DockerContainer(
      containerName = "my-test-container",
      hostPath = Some("/tmp/host"),
      containerPath = Some("/app/data"),
      imageName = Some("python:3.9-slim")
    )

    // 序列化 (toJson)
    val json = originalContainer.toJson()
    assertNotNull(json, "toJson() 返回的 JSONObject 不应为 null")

    assertEquals("my-test-container", json.getString("containerName"), "JSON 中的 containerName 不匹配")
    assertEquals("/tmp/host", json.getString("hostPath"), "JSON 中的 hostPath 不匹配")
    assertEquals("/app/data", json.getString("containerPath"), "JSON 中的 containerPath 不匹配")
    assertEquals("python:3.9-slim", json.getString("imageName"), "JSON 中的 imageName 不匹配")

    // 反序列化 (fromJson)
    val deserializedContainer = DockerContainer.fromJson(json)

    // 验证往返数据一致性
    assertEquals(originalContainer, deserializedContainer, "反序列化后的 DockerContainer 与原始对象不匹配")
  }

  /**
   * 测试 JSON 序列化 (toJson) 和反序列化 (fromJson) 的往返能力
   * 场景：只有必填字段（Option 字段为 None）
   */
  @Test
  def testJsonSerializationRoundTrip_Minimal(): Unit = {
    val originalContainer = DockerContainer(
      containerName = "minimal-container"
    )

    // 序列化 (toJson)
    val json = originalContainer.toJson()
    assertNotNull(json, "toJson() 返回的 JSONObject 不应为 null")

    assertEquals("minimal-container", json.getString("containerName"), "JSON 中的 containerName 不匹配")
    assertFalse(json.has("hostPath"), "JSON 中不应包含 hostPath")
    assertFalse(json.has("containerPath"), "JSON 中不应包含 containerPath")
    assertFalse(json.has("imageName"), "JSON 中不应包含 imageName")

    // 反序列化 (fromJson)
    val deserializedContainer = DockerContainer.fromJson(json)

    // 3. 验证往返数据一致性
    assertEquals(originalContainer, deserializedContainer, "反序列化后的 DockerContainer 与原始对象不匹配")
  }

  /**
   * 测试 fromJson 对部分缺失数据的处理
   */
  @Test
  def testFromJsonWithPartialData(): Unit = {
    val jo = new JSONObject()
    jo.put("containerName", "partial-container")
    jo.put("imageName", "alpine")

    val expectedContainer = DockerContainer(
      containerName = "partial-container",
      hostPath = None,
      containerPath = None,
      imageName = Some("alpine")
    )

    val deserializedContainer = DockerContainer.fromJson(jo)

    assertEquals(expectedContainer, deserializedContainer, "从部分 JSON 数据反序列化失败")
  }

  /**
   * 测试 start() 方法
   * 这是一个集成测试，它依赖于外部的 DockerExecute 对象，
   * 并且要求测试环境中必须安装并运行 Docker 守护进程。
   *
   * 默认禁用此测试，以防止在 CI/CD 或没有 Docker 的环境中失败。
   * 请在本地手动启用以进行验证。
   */
  @Test
  @Disabled("需要安装并运行 Docker 守护进程，并拉取 python:3.9-slim 镜像")
  def testStartMethod_Integration(): Unit = {
    // 准备一个唯一的容器名
    val containerName = s"dftp_test_${UUID.randomUUID().toString.take(8)}"
    val imageName = "python:3.9-slim" // 必须是一个本地可用的镜像
    val hostPath = new File(".").getCanonicalPath // 映射当前目录
    val containerPath = "/testdata"

    val container = DockerContainer(
      containerName,
      hostPath = Some(hostPath),
      containerPath = Some(containerPath),
      imageName = Some(imageName)
    )

    var resultContainerName: String = null
      // 第一次启动 (应该会创建并启动容器)
      resultContainerName = container.start()

      assertEquals(containerName, resultContainerName, "start() 第一次调用应返回容器名")
      assertTrue(DockerExecute.isContainerRunning(containerName), "容器在第一次启动后应处于运行状态")

      // 第二次启动 (容器已在运行，应直接返回名称)
      val resultContainerName2 = container.start()
      assertEquals(containerName, resultContainerName2, "start() 第二次调用应直接返回容器名")
  }

}