package link.rdcn.dacp.proxy

import link.rdcn.client.dacp.DacpClientProxy
import link.rdcn.server.{DftpServer, DftpServerConfig}
import link.rdcn.user.Credentials
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/5 16:57
 * @Modified By:
 */
object DacpClientProxyTest{

  var server: DftpServer = _

  @BeforeAll
  def startServer(): Unit = {
    server = DacpServerProxy.start("dacp://10.0.82.143:3102", DftpServerConfig("0.0.0.0")
      .withPort(3105)
      .withProtocolScheme("dacp"))
  }

  @AfterAll
  def close(): Unit = {
    server.close()
  }
}

class DacpClientProxyTest {

  @Test
  def getTest(): Unit = {
    val client = DacpClientProxy.connect("dacp://0.0.0.0:3112", Credentials.ANONYMOUS)
    println(client.getTargetServerUrl)
  }
}
