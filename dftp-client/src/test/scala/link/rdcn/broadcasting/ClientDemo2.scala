/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 17:04
 * @Modified By:
 */
package link.rdcn.broadcasting

import link.rdcn.client.DftpClient
import link.rdcn.struct._
import link.rdcn.user.UsernamePassword

object ClientDemo2 {
  val dc: DftpClient = DftpClient.connect("dftp://0.0.0.0:3102", UsernamePassword("admin@instdb.cn", "admin001"));

  def main(args: Array[String]): Unit = {
    // 通过用户名密码非加密连接FairdClient

  }

}
