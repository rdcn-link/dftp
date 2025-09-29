/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
package link.rdcn

import link.rdcn.ClientTestBase._
import link.rdcn.struct.StructType
import link.rdcn.struct.ValueType._
import link.rdcn.user.{Credentials, UserPrincipal, UsernamePassword}

import java.io.File
import java.nio.file.Paths

trait ClientTestDemoProvider {

}

object ClientTestDemoProvider {
  val baseDirString: String = demoBaseDir
  val subDirString: String = "data"
  ConfigLoader.init()

  val prefix = "dftp://" + ConfigLoader.dftpConfig.host + ":" + ConfigLoader.dftpConfig.port


  val baseDir = getOutputDir(baseDirString, subDirString)
  // 生成的临时目录结构
  val binDir = getOutputDir(baseDirString, Seq(subDirString, "bin").mkString(File.separator))
  val csvDir = getOutputDir(baseDirString, Seq(subDirString, "csv").mkString(File.separator))
  val excelDir = getOutputDir(baseDirString, Seq(subDirString, "excel").mkString(File.separator))

  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir).map(file => {
    DataFrameInfo(Paths.get("/csv").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(binDir).getFileName).toString.replace("\\", "/"), Paths.get(binDir).toUri, DirectorySource(false), StructType.binaryStructType))
  lazy val excelDfInfos = listFiles(excelDir).map(file => {
    DataFrameInfo(Paths.get("/excel").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, ExcelSource(), StructType.empty.add("id", IntType).add("value", IntType))
  })

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)
  val dataSetExcel = DataSet("excel", "3", excelDfInfos.toList)

  class TestAuthenticatedUser(userName: String, token: String) extends UserPrincipal {
    def getUserName: String = userName
  }

  val authProvider = new AuthProvider {

    override def authenticate(credentials: Credentials): UserPrincipal = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.username == null && usernamePassword.password == null) {
          sendErrorWithFlightStatus(401,"User not found!")
        }
        else if (usernamePassword.username == adminUsername && usernamePassword.password == adminPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        } else if (usernamePassword.username == userUsername && usernamePassword.password == userPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        }
        else if (usernamePassword.username != adminUsername) {
          sendErrorWithFlightStatus(401,"User unauthorized!")
        } else if (usernamePassword.username == adminUsername && usernamePassword.password != adminPassword) {
          sendErrorWithFlightStatus(401,"Wrong password!")
        }
        else
        {
          sendErrorWithFlightStatus(0,"User authenticate unknown error!")
        }
      } else if (credentials == Credentials.ANONYMOUS) {
        new TestAuthenticatedUser(anonymousUsername, genToken())
      }
      else {
        sendErrorWithFlightStatus(400,"Invalid credentials!")
      }
    }


    /**
     * 判断用户是否具有某项权限
     *
     * @param user          已认证用户
     * @param dataFrameName 数据帧名称
     * @param opList        操作类型列表（Java List）
     * @return 是否有权限
     */
    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[Object]): Boolean = true
  }

  val dataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      Paths.get(baseDir, relativePath).toString
    }
  }

}






