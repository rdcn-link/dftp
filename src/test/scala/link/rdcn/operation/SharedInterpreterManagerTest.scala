/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/27 09:40
 * @Modified By:
 */
package link.rdcn.operation

import link.rdcn.ConfigLoader
import link.rdcn.ConfigLoader.dftpConfig
import link.rdcn.log.LoggerFactory
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SharedInterpreterManagerTest {
  @Test
  def getJepTest(): Unit = {
    ConfigLoader.init()
    LoggerFactory.setDftpConfig(dftpConfig)
    val jep = SharedInterpreterManager.getInterpreter
    assertTrue(jep != null, "jep doesn't exit!")
    jep.close()
  }
}
