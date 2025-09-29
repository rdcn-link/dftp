/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 19:13
 * @Modified By:
 */
package link.rdcn.server

import link.rdcn.TestBase.getResourcePath
import link.rdcn.TestProvider
import link.rdcn.TestProvider.getServer
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.Paths

class DftpServerTest extends TestProvider{
  val server = getServer
  val tlsDir = getResourcePath("tls")

  @Test
  def testSetProtocolSchema(): Unit = {
    val newServer = server.setProtocolSchema("dftp")
    assertTrue(newServer eq server)
  }

  @Test
  def testEnableTLS(): Unit = {
    val certFile = new File(Paths.get(tlsDir,"server.crt").toString)
    val keyFile = new File(Paths.get(tlsDir,"server.pem").toString)
    val newServer = server.enableTLS(certFile, keyFile)
    assertTrue(newServer eq server)
  }

  @Test
  def testDisableTLS(): Unit = {
    val newServer = server.disableTLS()
    assertTrue(newServer eq server)
  }


}
