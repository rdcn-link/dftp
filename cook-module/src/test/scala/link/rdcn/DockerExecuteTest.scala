package link.rdcn

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.exception.{ConflictException, NotFoundException}
import com.github.dockerjava.api.model.{Bind, Volume}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import link.rdcn.optree.fifo.DockerExecute
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.nio.file.{Files, Path, Paths}

class DockerExecuteTest {

  // --- 根据新需求更新常量 ---
  private val JYG_CONTAINER_NAME = "jyg-container"
  private val JYG_IMAGE = "registry.cn-hangzhou.aliyuncs.com/cnic-piflow/siltdam-jyg:latest"

  private val HOST_DIR = "/data2/work/ncdc/faird/temp"
  private val CONTAINER_DIR = "/mnt/data"

  private val TEST_SCRIPT_FILE_NAME = "op1.py"
  private val TEST_SCRIPT_HOST_PATH: Path = Paths.get(HOST_DIR, "op1", TEST_SCRIPT_FILE_NAME)
  private val TEST_SCRIPT_CONTAINER_PATH = Paths.get(CONTAINER_DIR, "/op1", TEST_SCRIPT_FILE_NAME).toString

  private val LIFECYCLE_TEST_CONTAINER_NAME = "jyg-lifecycle-test"
  private val LIFECYCLE_TEST_IMAGE = JYG_IMAGE


  private var dockerClient: DockerClient = _

  /**
   * 在每个测试方法执行前运行。
   * 核心职责：确保名为 'jyg-container' 的容器正在运行，并准备好测试脚本。
   */
  @BeforeEach
  def setUp(): Unit = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    dockerClient = DockerClientBuilder.getInstance(config).build()

    // 确保目标镜像存在，如果不存在则拉取
    pullImageIfNotExists(JYG_IMAGE)

    // 确保宿主机挂载目录存在
    if (!Files.exists(Paths.get(HOST_DIR))) {
      Files.createDirectories(Paths.get(HOST_DIR))
    }

    // 使用 assertTrue 断言来判断文件是否存在
    assertTrue(Files.exists(TEST_SCRIPT_HOST_PATH), s"测试脚本文件 'gully_slop.py' 在预期的路径 ${TEST_SCRIPT_HOST_PATH.toString} 下未找到")

    // 检查 'jyg-container' 容器的状态并确保其正在运行
    ensureJygContainerIsRunning()
  }

  /**
   * 在每个测试方法执行后运行
   */
  @AfterEach
  def tearDown(): Unit = {
    if (dockerClient != null) {
      dockerClient.close()
    }
  }

  @Test
  def testStartContainerAndIsRunningLifecycle(): Unit = {
    // 为保证测试独立，先清理可能残留的上次运行失败的临时容器
    cleanupContainer(LIFECYCLE_TEST_CONTAINER_NAME)
    var containerId: String = null

    try {
      // 1. 验证启动前：isContainerRunning 应返回 false
      assertFalse(DockerExecute.isContainerRunning(LIFECYCLE_TEST_CONTAINER_NAME),
        s"启动前, 临时容器 '$LIFECYCLE_TEST_CONTAINER_NAME' 不应处于运行状态")

      // 2. 执行被测方法：startContainer
      containerId = DockerExecute.startContainer(HOST_DIR, CONTAINER_DIR, LIFECYCLE_TEST_CONTAINER_NAME, LIFECYCLE_TEST_IMAGE)
      assertNotNull(containerId, "startContainer 方法应返回一个非空的容器 ID")
      assertNotEquals("", containerId.trim, "返回的容器 ID 不应是空字符串")

      // 3. 验证启动后：isContainerRunning 应返回 true
      assertTrue(DockerExecute.isContainerRunning(LIFECYCLE_TEST_CONTAINER_NAME),
        s"调用 startContainer 后, 临时容器 '$LIFECYCLE_TEST_CONTAINER_NAME' 应处于运行状态")

      // 4. 手动停止容器，以测试 isContainerRunning 在容器停止后的行为
      dockerClient.stopContainerCmd(containerId).exec()

      // 5. 验证停止后：isContainerRunning 应再次返回 false
      assertFalse(DockerExecute.isContainerRunning(LIFECYCLE_TEST_CONTAINER_NAME),
        s"容器停止后, isContainerRunning 方法应返回 false")

    } finally {
      // 6. 确保测试创建的临时容器被彻底清理（无论测试成功或失败）
      cleanupContainer(LIFECYCLE_TEST_CONTAINER_NAME)
    }
  }

  /**
   * 测试在 'jyg-container' 内执行一个简单的 echo 命令
   */
  @Test
  def testNonInteractiveEchoExec(): Unit = {
    assertTrue(DockerExecute.isContainerRunning(JYG_CONTAINER_NAME), s"执行测试前, 容器 '$JYG_CONTAINER_NAME' 必须是运行状态")

    val command = Array("echo", "Hello from jyg-container")
    val expectedOutput = "Hello from jyg-container"
    val fullOutput = DockerExecute.nonInteractiveExec(command, JYG_CONTAINER_NAME)

    assertTrue(fullOutput.contains(expectedOutput), s"输出内容应包含期望的字符串 '$expectedOutput'")
  }

  /**
   * 测试在 'jyg-container' 内执行指定的 Python 脚本
   */
  @Test
  def testNonInteractivePythonScriptExec(): Unit = {
    assertTrue(DockerExecute.isContainerRunning(JYG_CONTAINER_NAME), s"执行测试前, 容器 '$JYG_CONTAINER_NAME' 必须是运行状态")

    val command = Array("python", TEST_SCRIPT_CONTAINER_PATH)
    val fullOutput = DockerExecute.nonInteractiveExec(command, JYG_CONTAINER_NAME)
    assertTrue(fullOutput.nonEmpty, s"Python 脚本应具有输出")

  }

  // --- 辅助方法 ---

  /**
   * 安全地停止并移除一个容器，用于清理临时测试容器。
   */
  private def cleanupContainer(containerName: String): Unit = {
    try {
      // 1. 检查容器是否存在
      val inspectResponse = dockerClient.inspectContainerCmd(containerName).exec()

      // 2. 如果容器存在，检查其运行状态
      val isRunning: Boolean = Option(inspectResponse.getState)
        .flatMap(state => Option(state.getRunning)).exists(_.booleanValue)

      // 3. 只有在容器正在运行时才执行停止操作
      if (isRunning) {
        println(s"正在停止运行中的容器: $containerName")
        dockerClient.stopContainerCmd(containerName).exec()
      }

      // 4. 确保容器最终被移除（此时容器肯定是停止状态）
      println(s"正在移除容器: $containerName")
      dockerClient.removeContainerCmd(containerName).exec()

    } catch {
      case _: NotFoundException =>
      // 容器不存在，说明已经清理干净了，这是正常情况，无需任何操作。
      case e: Exception =>
        System.err.println(s"清理容器 '$containerName' 时发生未预料的错误: ${e.getMessage}")
    }
  }

  /**
   * 核心逻辑：检查 jyg-container 状态，如果不存在则创建，如果已停止则启动
   */
  private def ensureJygContainerIsRunning(): Unit = {
    try {
      val inspectResponse = dockerClient.inspectContainerCmd(JYG_CONTAINER_NAME).exec()
      if (!inspectResponse.getState.getRunning) {
        println(s"容器 '$JYG_CONTAINER_NAME' 已存在但处于停止状态，正在启动...")
        dockerClient.startContainerCmd(JYG_CONTAINER_NAME).exec()
      } else {
        println(s"容器 '$JYG_CONTAINER_NAME' 已在运行中。")
      }
    } catch {
      case _: NotFoundException =>
        println(s"容器 '$JYG_CONTAINER_NAME' 不存在，将根据指定命令创建并启动...")
        try {
          // 严格按照指定的启动命令创建容器
          val container = dockerClient.createContainerCmd(JYG_IMAGE)
            .withName(JYG_CONTAINER_NAME)
            .withBinds(new Bind(HOST_DIR, new Volume(CONTAINER_DIR)))
            .withTty(true) // 对应 -t
            .withStdinOpen(true) // 对应 -i
            .withCmd("/bin/bash") // 指定启动命令
            .exec()

          dockerClient.startContainerCmd(container.getId).exec()
          println(s"容器 '$JYG_CONTAINER_NAME' 已成功创建并启动。")
        } catch {
          case e: ConflictException =>
            fail(s"创建容器 '$JYG_CONTAINER_NAME' 时发生命名冲突，这不应该发生。请检查 Docker 环境。", e)
          case e: Exception =>
            fail(s"创建并启动容器 '$JYG_CONTAINER_NAME' 失败。", e)
        }
    }
  }

  /**
   * 检查指定的 Docker 镜像是否存在，如果不存在则从仓库拉取
   */
  private def pullImageIfNotExists(imageName: String): Unit = {
    try {
      dockerClient.inspectImageCmd(imageName).exec()
      println(s"镜像 '$imageName' 已在本地存在。")
    } catch {
      case _: NotFoundException =>
        println(s"镜像 '$imageName' 在本地未找到，正在从仓库拉取...")
        try {
          dockerClient.pullImageCmd(imageName).start().awaitCompletion()
          println(s"成功拉取镜像 '$imageName'。")
        } catch {
          case e: Exception =>
            fail(s"拉取镜像 '$imageName' 失败，测试无法继续。错误: ${e.getMessage}")
        }
    }
  }
}