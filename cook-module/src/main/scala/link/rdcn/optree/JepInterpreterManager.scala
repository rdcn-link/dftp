package link.rdcn.optree

import jep._
import link.rdcn.Logging

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.{Files, Paths}
import scala.sys.process._

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 18:30
 * @Modified By:
 */

object JepInterpreterManager extends Logging {
  def getJepInterpreter(sitePackagePath: String, whlPath: String, pythonHome: Option[String]=None): SubInterpreter = {
    try {
      //将依赖环境安装到指定目录
      val env: (String, String) = pythonHome.map("PATH" -> _)
        .getOrElse("PATH" -> sys.env.getOrElse("PATH", ""))
      val cmd = Seq(getPythonExecutablePath(env._2), "-m", "pip", "install", "--upgrade", "--target", sitePackagePath, whlPath)
      val output = Process(cmd, None, env).!!
      logger.debug(output)
      logger.debug(s"Python dependency from '$whlPath' has been successfully installed to '$sitePackagePath'.")
      val config = new JepConfig
      config.addIncludePaths(sitePackagePath).setClassEnquirer(new JavaUtilOnlyClassEnquirer)
      new SubInterpreter(config)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }

  }

  private def getPythonExecutablePath1(env: String): String = {
    // 1. 获取当前进程的 PATH 环境变量，考虑 PYTHONHOME 作为备选
    // 2. 尝试从 CONDA_PREFIX 构造 Anaconda 环境的 bin/Scripts 目录，并添加到 PATH 前面
    val condaPrefixPath = sys.env.get("CONDA_PREFIX") match {
      case Some(prefix) =>
        // Windows 上通常是 envs/name/Scripts 和 envs/name
        // Linux/macOS 上是 envs/name/bin
        val binPath = Paths.get(prefix, "bin").toString
        val scriptsPath = Paths.get(prefix, "Scripts").toString // Windows 特有
        // 确保路径不重复
        Seq(binPath, scriptsPath).filter(p => Files.exists(Paths.get(p)))
      case None => Seq.empty[String]
    }

    // 3. 将 PATH 字符串拆分为单独的目录，并去重
    // Windows 路径分隔符是 ';', Unix-like 是 ':'
    val pathSeparator = sys.props("path.separator")
    val pathDirs = (env.split(pathSeparator) ++ condaPrefixPath).distinct // 合并并去重

    val commonPythonExecutables = Seq("python.exe", "python3.exe") // Windows 可执行文件名
    val executables = if (sys.props("os.name").toLowerCase.contains("linux") || sys.props("os.name").toLowerCase.contains("mac")) {
      // Unix-like 系统上的可执行文件名通常没有 .exe 后缀
      // 这里的 .reverse.foreach 和 .filter(Files.exists) 保证了查找的健壮性
      commonPythonExecutables.map(_.stripSuffix(".exe")) // 移除 .exe 后缀
    } else {
      commonPythonExecutables
    }


    for (dir <- pathDirs) {
      val dirPath = Paths.get(dir)
      if (Files.exists(dirPath)) { // 确保目录存在
        for (execName <- commonPythonExecutables) {
          val fullPath = dirPath.resolve(execName)
          if (Files.exists(fullPath) && Files.isRegularFile(fullPath) && Files.isExecutable(fullPath)) {
            //            println(s"Found Python executable at: ${fullPath.toString}")
            return fullPath.toString // 找到并返回第一个
          }
        }
      }
    }

    // 如果遍历所有 PATH 目录后仍未找到
    val errorMessage = "Failed to find 'python.exe' or 'python3.exe' in any PATH directory. " +
      "Please ensure Python is installed and its executable is in your system's PATH, " +
      "or the CONDA_PREFIX environment variable points to a valid Anaconda environment."
    println(s"Error: $errorMessage")
    sys.error(errorMessage) // 抛出错误终止程序
  }

  private def getPythonExecutablePath(env: String): String = {
    // 1. Get the current OS
    val osName = sys.props("os.name").toLowerCase
    val isWindows = osName.contains("windows")
    val isUnixLike = !isWindows

    // 2. Handle conda environment paths
    val condaPrefixPath = sys.env.get("CONDA_PREFIX") match {
      case Some(prefix) =>
        if (isWindows) {
          // Windows: check both Scripts and bin directories
          val scriptsPath = Paths.get(prefix, "Scripts").toString
          val binPath = Paths.get(prefix, "bin").toString
          Seq(scriptsPath, binPath).filter(p => Files.exists(Paths.get(p)))
        } else {
          // Unix-like: only check bin directory
          val binPath = Paths.get(prefix, "bin").toString
          if (Files.exists(Paths.get(binPath))) Seq(binPath) else Seq.empty
        }
      case None => Seq.empty[String]
    }

    // 3. Handle PYTHONHOME if present
    val pythonHomePath = sys.env.get("PYTHONHOME").map { home =>
      if (isWindows) {
        Seq(Paths.get(home).toString)
      } else {
        Seq(Paths.get(home, "bin").toString)
      }
    }.getOrElse(Seq.empty)

    // 4. Prepare all possible executable names based on platform
    val executableNames = if (isWindows) {
      Seq("python.exe", "python3.exe", "python")
    } else {
      Seq("python3", "python", "python3.9", "python3.8", "python3.7", "python2.7")
    }

    // 5. Prepare PATH directories with priority to conda and python home
    val pathSeparator = sys.props("path.separator")
    val pathDirs = (condaPrefixPath ++ pythonHomePath ++ env.split(pathSeparator)).distinct

    // 6. Search for the executable
    for (dir <- pathDirs) {
      val dirPath = Paths.get(dir)
      if (Files.exists(dirPath)) {
        for (execName <- executableNames) {
          val fullPath = dirPath.resolve(execName)
          if (Files.exists(fullPath)) {
            if (isUnixLike) {
              // On Unix-like systems, check if it's executable
              try {
                if (Files.isExecutable(fullPath)) {
                  return fullPath.toString
                }
              } catch {
                case _: SecurityException => // Ignore if we can't check permissions
              }
            } else {
              // On Windows, just check if the file exists
              return fullPath.toString
            }
          }
        }
      }
    }

    // 7. Fallback to trying which/where command
    val fallbackCommand = if (isWindows) "where python" else "which python3 || which python"
    try {
      val process = Runtime.getRuntime.exec(fallbackCommand)
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      Option(reader.readLine()).map(_.trim).filterNot(_.isEmpty) match {
        case Some(path) if Files.exists(Paths.get(path)) => return path
        case _ => // Continue to error
      }
    } catch {
      case _: Exception => // Ignore if command fails
    }

    // 8. If nothing found, throw error
    val errorMessage =
      s"""Failed to find Python executable in any PATH directory.
         |Searched in: ${pathDirs.mkString(", ")}
         |Tried executable names: ${executableNames.mkString(", ")}
         |Please ensure Python is installed and available in your system's PATH,
         |or set CONDA_PREFIX/PYTHONHOME environment variables correctly.""".stripMargin
    sys.error(errorMessage)
  }

  private class JavaUtilOnlyClassEnquirer extends ClassEnquirer {

    override def isJavaPackage(name: String): Boolean = name == "java.util" || name.startsWith("java.util.")

    override def getClassNames(pkgName: String): Array[String] = Array.empty

    override def getSubPackages(pkgName: String): Array[String] = Array.empty
  }
}