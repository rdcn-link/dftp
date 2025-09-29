/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/25 17:04
 * @Modified By:
 */
package link.rdcn.demo

import link.rdcn.ClientTestBase.testClassesDir
import link.rdcn.client.DftpClient
import link.rdcn.struct._
import link.rdcn.user.UsernamePassword
import link.rdcn.util.CodecUtils
import org.apache.commons.io.IOUtils

import java.io.{File, FileOutputStream}
import java.nio.file.{Path, Paths}

object ClientDemo {
  def main(args: Array[String]): Unit = {
    // 通过用户名密码非加密连接FairdClient
        val dc: DftpClient = DftpClient.connect("dftp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"));
    // 通过用户名密码tls加密连接FairdClient
    //    val dc: DftpClient = DftpClient.connectTLS("dftp://localhost:3101", tlsFile, UsernamePassword("admin@instdb.cn", "admin001"))
    // 匿名连接FairdClient
    //    val dcAnonymous: DftpClient = DftpClient.connect("dftp://localhost:3101", Credentials.ANONYMOUS());

    /**
     * get操作从server获得url对应的DataFrame数据
     * 需要server在doGet实现
     */
    val dfCsv: DataFrame = dc.get("dftp://localhost:3101/csv/data_1.csv")
    val csvRows: Seq[Row] = dfCsv.limit(1).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 数据帧--------------")
    csvRows.foreach(println)
    //编写map算子的匿名函数对数据帧进行操作
    val mappedRows: Seq[Row] = dfCsv.map(x => Row(x._1)).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过map操作后的数据帧--------------")
    mappedRows.take(3).foreach(println)

    //编写filter算子的匿名函数对数据帧进行操作
    val filteredRows: Seq[Row] = dfCsv.filter({ row =>
      val id: Long = row._1.asInstanceOf[Long]
      id <= 1L
    }).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过filter操作后的数据帧--------------")
    filteredRows.foreach(println)

    //select可以通过列名得到指定列的数据
    val selectedRows: Seq[Row] = dfCsv.select("id").collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过select操作后的数据帧--------------")
    selectedRows.take(3).foreach(println)

    /**
     * 打开非结构化数据的文件列表数据帧
     * 可以对数据帧进行操作 比如foreach 每行数据为一个Row对象，可以通过Tuple风格访问每一列的值
     */
    //
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
      val path: Path = Paths.get(testClassesDir.getParentFile.getParentFile.getAbsolutePath, "src", "test", "resources", "data", "output", name)
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

    /**
     * get操作也可以从server获得url对应的自定义信息如服务器资源信息
     * 需要server在doGet实现
     */
    val dfListHostResourceInfo: DataFrame = dc.get("dftp://localhost:3101/listHostResourceInfo")
    println("--------------打印服务器资源信息--------------")
    dfListHostResourceInfo.foreach(println)

    /**
     * put操作用于上传DataFrame
     * 可以通过文件路径获得的DataStreamSource创建DataFrame
     * 通过CodecUtils.decodeString获得响应内容
     */
    val dataStreamSource: DataStreamSource = DataStreamSource.filePath(new File(""))
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    val batchSize = 100
    val ackBytes:Array[Byte] = dc.put(dataFrame, batchSize)
    println("--------------打印上传数据后的响应--------------")
    println(CodecUtils.decodeString(ackBytes))

  }

}
