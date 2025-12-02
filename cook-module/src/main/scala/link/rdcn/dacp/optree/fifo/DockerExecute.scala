package link.rdcn.dacp.optree.fifo

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.CreateContainerResponse
import com.github.dockerjava.api.model.{Bind, Container, Volume}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import com.github.dockerjava.api.model.{Frame, StreamType}
import com.github.dockerjava.core.command.ExecStartResultCallback

import scala.collection.JavaConverters._

object DockerExecute {

  /**
   * 检查指定名称的容器是否正在运行
   * @param containerName 容器名称（例如 "jyg-container"）
   * @return Boolean 是否运行中
   */
  def isContainerRunning(containerName: String): Boolean =
  {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    try {
      // 列出所有容器（包括未运行的）
      val containers: List[Container] = dockerClient.listContainersCmd()
        .withShowAll(true) // 显示所有容器（包括已停止的）
        .exec()
        .asScala
        .toList

      // 按名称过滤并检查状态
      containers.exists { container =>
        container.getNames.contains(s"/$containerName") &&
          container.getState.equalsIgnoreCase("running")
      }
    } finally {
      dockerClient.close()
    }
  }

  /**
   * 启动一个交互式容器，并挂载目录
   *
   * @param hostDir     宿主机目录（如 `/data2/work/ncdc/faird/temp`）
   * @param containerDir 容器内挂载目录（如 `/mnt/data`）
   * @param containerName 容器名称（如 `jyg-container`）
   * @param imageName    镜像名称（如 `registry.cn-hangzhou.aliyuncs.com/cnic-piflow/siltdam-jyg:latest`）
   * @return 容器ID
   */
  def startContainer(
                      hostDir: String,
                      containerDir: String,
                      containerName: String,
                      imageName: String
                    ): String = {
    // 1. 配置 Docker 客户端
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    try {
      // 2. 创建容器（挂载目录 + 交互式终端）
      val container: CreateContainerResponse = dockerClient.createContainerCmd(imageName)
        .withName(containerName)
        .withBinds(new Bind(hostDir, new Volume(containerDir))) // 挂载目录
//        .withBinds(new Bind("/dev/shm", new Volume("/dev/shm"))) // 挂载目录
        .withTty(true)    // 相当于 -t
        .withStdinOpen(true) // 相当于 -i
        .exec()

      // 3. 启动容器
      dockerClient.startContainerCmd(container.getId).exec()
      println(s"容器已启动，ID: ${container.getId}")

      container.getId
    } finally {
      dockerClient.close()
    }
  }

  // Array("python","op1.py")
  def nonInteractiveExec(command: Array[String], containerId: String): String = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    //    val containerId = "jyg-container"
    //    val command = Array("python", "/mnt/data/script.py")

    try {
      val execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
        .withCmd(command: _*)
        .withAttachStdin(true)   // 相当于 -i 参数
        .withAttachStdout(true)
        .withAttachStderr(true)
        .exec()

      val output = new StringBuilder
      val callback = new ExecStartResultCallback() {
        override def onNext(frame: Frame): Unit = {
          frame.getStreamType match {
            case StreamType.STDOUT | StreamType.STDERR =>
              output.append(new String(frame.getPayload))
            case _ => // 忽略其他类型
          }
          super.onNext(frame)
        }
      }

      dockerClient.execStartCmd(execCreateCmdResponse.getId)
        .withDetach(false)
        .withTty(false)  // 禁用TTY (相当于去掉 -t 参数)
        .exec(callback)
        .awaitCompletion()

      println("命令执行结果:")
      println(output.toString())
      output.toString()

    } finally {
      dockerClient.close()
    }
  }

  /**
   * 停止并删除指定容器
   * @param containerName 容器名称
   * @param forceDelete   是否强制删除（即使容器未停止）
   * @return 是否成功删除
   */
  def stopAndRemoveContainer(containerName: String, forceDelete: Boolean = true): Boolean = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    try {
      // 查找容器（包括已停止的）
      val containers: List[Container] = dockerClient.listContainersCmd()
        .withShowAll(true)
        .exec()
        .asScala
        .toList

      containers.find(_.getNames.contains(s"/$containerName")) match {
        case Some(container) =>
          val containerId = container.getId

          //  停止容器
          if (container.getState.equalsIgnoreCase("running")) {
            println(s"Stopping container $containerName ...")
            dockerClient.stopContainerCmd(containerId).exec()
          }

          // 删除容器
          println(s"Removing container $containerName ...")
          dockerClient.removeContainerCmd(containerId)
            .withForce(forceDelete) // 强制删除
            .exec()

          println(s"Container $containerName removed successfully.")
          true

        case None =>
          println(s"Container $containerName does not exist.")
          false
      }
    } finally {
      dockerClient.close()
    }
  }

  /**
   * 上传并覆盖容器内的文件，例如 Python 脚本等
   *
   * @param containerId   已启动容器的 ID 或名称
   * @param localFilePath 本地文件路径，如 /data/local/overlap_dam_select.py
   * @param containerPath 容器内路径，如 /dem/overlap_dam_select.py
   */
  def uploadFileToContainer(containerId: String, localFilePath: String, containerPath: String): Unit = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val dockerClient: DockerClient = DockerClientBuilder.getInstance(config).build()

    try {
      val localFile = new java.io.File(localFilePath)
      require(localFile.exists(), s"本地文件不存在: $localFilePath")

      // Docker SDK 要求上传的是 TAR 格式
      val tarFile = java.io.File.createTempFile("upload_", ".tar")

      // 创建 TAR 文件
      val fos = new java.io.FileOutputStream(tarFile)
      val tos = new org.apache.commons.compress.archivers.tar.TarArchiveOutputStream(fos)

      try {
        val entry = new org.apache.commons.compress.archivers.tar.TarArchiveEntry(localFile.getName)
        entry.setSize(localFile.length())
        tos.putArchiveEntry(entry)

        val fis = new java.io.FileInputStream(localFile)
        try {
          val buffer = new Array[Byte](8 * 1024)
          Iterator
            .continually(fis.read(buffer))
            .takeWhile(_ != -1)
            .foreach(read => tos.write(buffer, 0, read))
        } finally {
          fis.close()
        }

        tos.closeArchiveEntry()
      } finally {
        tos.close()
        fos.close()
      }

      // 上传 TAR 到容器 → 覆盖 containerPath 所在目录
      val parentDir =
        if (containerPath.contains("/")) containerPath.substring(0, containerPath.lastIndexOf("/"))
        else "/"

      dockerClient.copyArchiveToContainerCmd(containerId)
        .withRemotePath(parentDir)
        .withTarInputStream(new java.io.FileInputStream(tarFile))
        .exec()

      println(s"已上传并覆盖容器文件：$localFilePath → $containerId:$containerPath")

    } finally {
      dockerClient.close()
    }
  }

}
