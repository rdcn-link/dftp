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
import org.apache.commons.io.IOUtils

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}

object ClientDemo1 {
  def main(args: Array[String]): Unit = {
    // 通过用户名密码非加密连接FairdClient
    val dc: DftpClient = DftpClient.connect("dftp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"));
    // 通过用户名密码tls加密连接FairdClient
    //    val dc: DftpClient = DftpClient.connectTLS("dftp://localhost:3101", tlsFile, UsernamePassword("admin@instdb.cn", "admin001"))
    // 匿名连接FairdClient
    //    val dcAnonymous: DftpClient = DftpClient.connect("dftp://localhost:3101", Credentials.ANONYMOUS());


    val dfBin: DataFrame = dc.get("dftp://localhost:3101/bin")
    println("--------------打印非结构化数据文件列表数据帧--------------")
    dfBin.foreach((row: Row) => {
      //通过Tuple风格访问
      val name: String = row._1.asInstanceOf[String]
      //通过下标访问
      val blob: Blob = row.get(6).asInstanceOf[Blob]
      //通过getAs方法获取列值，该方法返回Option类型，如果找不到对应的列则返回None
      val byteSize: Long = row.getAs[Long](3)
      //除此之外列值支持的类型还包括：Integer, Long, Float, Double, Boolean, byte[]
      //offerStream用于接受一个用户编写的处理blob InputStream的函数并确保其关闭
      val path: Path = Paths.get("C:\\Users\\Yomi\\PycharmProjects\\dftp-new\\dftp\\dftp-client\\target\\test-classes\\data\\output\\1.bin")
      blob.offerStream(inputStream => {
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)
        outputStream.close()
      })
      //或者直接获取blob的内容，得到byte数组
      println(row)
      println(name)
      println(byteSize)
    })
  }

}
