/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/27 09:40
 * @Modified By:
 */
package link.rdcn.operation

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SharedInterpreterManagerTest {
  @Test
  def getJepTest(): Unit = {
    val jep = SharedInterpreterManager.getInterpreter
    assertTrue(jep != null, "jep doesn't exit!")
  }
}
