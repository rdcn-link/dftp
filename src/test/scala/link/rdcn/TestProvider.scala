/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.TestDemoProvider.{baseDir, dataSetBin, dataSetCsv}
import link.rdcn.client.DftpClient
import link.rdcn.server.dftp.DftpServer
import link.rdcn.server._
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, StructType}
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.user.{Credentials, UserPrincipal, UsernamePassword}
import link.rdcn.util.CodecUtils
import org.json.JSONObject
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.nio.file.Paths

trait TestProvider {

}

object TestProvider {
  ConfigLoader.init()

  val prefix = "dftp://" + ConfigLoader.dftpConfig.host + ":" + ConfigLoader.dftpConfig.port
  val permissions = Map(
    adminUsername -> Set("/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/csv/data_1.csv", "/csv/invalid.csv", "/excel/data.xlsx")
//      .map(path => prefix + path)
  )

  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = Paths.get(baseDir, "bin").toString
  val csvDir = Paths.get(baseDir, "csv").toString

  //必须在DfInfos前执行一次
  TestDataGenerator.generateTestData(binDir, csvDir, baseDir)

  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir).map(file => {
    DataFrameInfo(Paths.get("/csv").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(binDir).getFileName).toString.replace("\\", "/"), Paths.get(binDir).toUri, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

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

  val dftpMethodService: DftpMethodService = new DftpMethodService {
    override def doGet(request: GetRequest, response: GetResponse): Unit = {
      request.getRequestURI() match {
        case otherPath =>
          //数据请求实现
          val userPrincipal = request.getUserPrincipal()
          //实现authProvider用于数据鉴权
          if(authProvider.checkPermission(userPrincipal, otherPath)){
            //实现dataProvider用于从url中解析DataStreamSource
            val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(otherPath)
            val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
            response.sendDataFrame(dataFrame)
          }else{
            response.sendError(403, s"access dataFrame $otherPath Forbidden")
          }
      }
    }

    override def doPut(request: PutRequest, response: PutResponse): Unit = {
      response.send(CodecUtils.encodeString(new JSONObject().put("status","success").toString))
    }

    override def doAction(request: ActionRequest, response: ActionResponse): Unit =
      response.send(CodecUtils.encodeString(new JSONObject().put("status","success").toString))
  }

  private var server: Option[DftpServer] = None
  var dc: DftpClient = _
  val configCache = ConfigLoader.dftpConfig
  var expectedHostInfo: Map[String, String] = _

  @BeforeAll
  def startServer(): Unit = {
    TestDataGenerator.generateTestData(binDir, csvDir, baseDir)
    getServer
    connectClient

  }

  @AfterAll
  def stop(): Unit = {
    stopServer()
    BlobRegistry.cleanUp()
    TestDataGenerator.cleanupTestData(baseDir)
  }

  def getServer: DftpServer = synchronized {
    if (server.isEmpty) {
      ConfigLoader.init()
      val s = new DftpServer(authProvider, dftpMethodService)
      s.start(ConfigLoader.dftpConfig)
      server = Some(s)

    }
    server.get
  }

  def connectClient: Unit = synchronized {
    dc = DftpClient.connect("dacp://localhost:3101", UsernamePassword(adminUsername, adminPassword))
  }

  def stopServer(): Unit = synchronized {
    server.foreach(_.close())
    server = None
  }

  val dataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      Paths.get(baseDir, relativePath).toString
    }
  }
}





