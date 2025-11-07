/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/6 15:02
 * @Modified By:
 */
package link.rdcn.optree

import jep.{JepException, SubInterpreter}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.io.File
import java.nio.file.Paths

class JepInterpreterManagerTest {

  @TempDir
  var tempDir: File = _

  private var sitePackagePath: String = _
  private var validWhlPath: String = _
  private var invalidWhlPath: String = _
  private var pythonHome: Option[String] = None

  @BeforeEach
  def setUp(): Unit = {
    // 创建一个唯一的 site-packages 目录供 pip 安装
    val sitePackageDir = new File(tempDir, "site-packages")
    sitePackageDir.mkdirs()
    sitePackagePath = sitePackageDir.getCanonicalPath

    // 获取有效的 .whl 文件路径 (从 test resources)
    try {
      val whlUri = getClass.getClassLoader.getResource("lib/link-0.1-py3-none-any.whl").toURI
      validWhlPath = Paths.get(whlUri).toString
    } catch {
      case e: Exception =>
        println(s"警告：无法加载 'lib/link-0.1-py3-none-any.whl'。集成测试 'testGetJepInterpreter_SuccessfulInstallAndRun' 将会失败。")
        validWhlPath = "invalid.whl" // 设为无效路径
    }

    // 3. 准备一个无效的 .whl 路径
    invalidWhlPath = Paths.get(tempDir.getCanonicalPath, "nonexistent-file.whl").toString

    // 4. (可选) 尝试获取 python.home，如果 JEP 测试已配置
    pythonHome = Option(System.getProperty("python.home"))
    if(pythonHome.isEmpty) {
      println("提示：'python.home' Java 属性未设置。JepInterpreterManager 将依赖系统 PATH 或 CONDA_PREFIX 查找 Python。")
    }
  }

  /**
   * 测试 getJepInterpreter 的“黄金路径”
   * 验证： 找到 Python
   * Pip install 成功
   * 3. SubInterpreter 创建成功
   * 4. JEP 可以执行代码
   * 5. 安装的 .whl 包可以被导入
   */
  @Test
  @Disabled("集成测试：需要一个完整的 Python + Pip + JEP 本机库环境")
  def testGetJepInterpreter_SuccessfulInstallAndRun(): Unit = {
    var interp: SubInterpreter = null
    try {
      // 执行
      interp = JepInterpreterManager.getJepInterpreter(sitePackagePath, validWhlPath, pythonHome)

      assertNotNull(interp, "getJepInterpreter 不应返回 null")

      // 验证 pip install 是否有效 (通过导入 .whl 中的模块)
      interp.exec("import link.rdcn.operators.registry as registry")

      // 验证 JEP 是否工作
      interp.exec("x = 10 + 5")
      val result = interp.getValue("x", classOf[java.lang.Integer])

      assertEquals(15, result, "JEP 解释器未能正确执行 Python 代码")

    } finally {
      if (interp != null) {
        interp.close()
      }
    }
  }

  /**
   * 测试 getJepInterpreter 返回的 SubInterpreter 是否正确应用了
   * JavaUtilOnlyClassEnquirer (只允许 java.util)
   */
  @Test
  @Disabled("集成测试：依赖于 testGetJepInterpreter_SuccessfulInstallAndRun 的成功")
  def testGetJepInterpreter_ClassEnquirerWorks(): Unit = {
    var interp: SubInterpreter = null
    try {
      interp = JepInterpreterManager.getJepInterpreter(sitePackagePath, validWhlPath, pythonHome)
      assertNotNull(interp, "getJepInterpreter 不应返回 null")

      // 测试允许的包 (java.util)
      // 这一行不应抛出异常
      interp.exec("from java.util import ArrayList")

      // 测试禁止的包 (java.io)
      val ex = assertThrows(classOf[JepException], () => {
        interp.exec("from java.io import File")
      }, "导入 java.io 应被 JavaUtilOnlyClassEnquirer 阻止并抛出 JepException")

      assertTrue(ex.getMessage.contains("java.io"), "JepException 消息应提及 'java.io'")

    } finally {
      if (interp != null) {
        interp.close()
      }
    }
  }

  /**
   * 测试当 pip install 失败时（因为 .whl 路径无效）
   * getJepInterpreter 是否会按预期抛出异常
   */
  @Test
  def testGetJepInterpreter_FailsOnBadWhlPath(): Unit = {
    // 假设：getPythonExecutablePath() 成功
    // 预期：Process(...).!! 失败并抛出 RuntimeException

    val ex = assertThrows(classOf[RuntimeException], () => {
      JepInterpreterManager.getJepInterpreter(sitePackagePath, invalidWhlPath, pythonHome)
      ()
    }, "使用无效的 .whl 路径调用 getJepInterpreter 应抛出 RuntimeException")

    // 验证它是否是 pip install 失败
    assertTrue(ex.getMessage.contains("Nonzero exit value"),
      "异常消息应表明 pip install 进程执行失败")
  }

  /**
   * 测试 getJepInterpreter 在 pythonHome 无效且 PATH 中没有 Python 时的失败情况
   * (注意：此测试很脆弱，如果 CI/CD 环境 *确实* 有 Python，它会失败)
   */
  @Test
  @Disabled("此测试很脆弱：它假设测试运行器环境中没有 python, CONDA_PREFIX 或 PYTHONHOME")
  def testGetPythonExecutablePath_FailsGracefully(): Unit = {
    // 准备一个在任何系统上都不太可能包含 python 的 PATH
    val minimalPath = System.getProperty("java.home") // e.g., "C:\jdk\jre"
    val mockPythonHome = Some(minimalPath) // 强制 JEP Manager 只查看这里

    // 执行并验证
    // 预期：getPythonExecutablePath 找不到 python 并调用 sys.error
    val ex = assertThrows(classOf[RuntimeException], () => { // sys.error 抛出 RuntimeException
      JepInterpreterManager.getJepInterpreter(sitePackagePath, validWhlPath, mockPythonHome)
      ()
    }, "在无效的 pythonHome 中查找 Python 应抛出 RuntimeException (sys.error)")

    assertTrue(ex.getMessage.toLowerCase.contains("failed to find python executable"),
      "异常消息应表明未能找到 Python")
  }
}