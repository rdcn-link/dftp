/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/27 09:40
 * @Modified By:
 */
package link.rdcn.server

import link.rdcn.ConfigLoader
import link.rdcn.ConfigLoader.dftpConfig
import link.rdcn.log.LoggerFactory
import link.rdcn.operation.SharedInterpreterManager
import org.junit.jupiter.api.Test

class JepInterpreterManagerTest {
  @Test
  def getJepTest(): Unit = {
    ConfigLoader.init()
    LoggerFactory.setDftpConfig(dftpConfig)
    val jep = SharedInterpreterManager.getInterpreter
    jep.close()
  }
}
