package link.rdcn.server

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/12 13:33
 * @Modified By:
 */
class ModulesBean {
  private var modules: Array[DftpModule] = Array.empty

  def setModules(modules: Array[DftpModule]): Unit = {
    this.modules = modules
  }

  def getModules: Array[DftpModule] = modules
}
