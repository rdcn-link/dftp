package link.rdcn

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/29 17:30
 * @Modified By:
 */
import link.rdcn.CommonTestBase._
import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.File
import java.nio.file.Paths

trait CommonTestProvider {

}

object CommonTestProvider {
  ConfigLoader.init()

  val prefix = "dftp://" + ConfigLoader.dftpConfig.host + ":" + ConfigLoader.dftpConfig.port
  val permissions = Map(
    adminUsername -> Set("/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/csv/data_1.csv", "/csv/invalid.csv", "/excel/data.xlsx")
    //      .map(path => prefix + path)
  )
  val baseDirString = Paths.get( "src", "test", "resources").toString
  val subDirString: String = "data"

  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = Paths.get(baseDir, "bin").toString
  val csvDir = Paths.get(baseDir, "csv").toString
  val excelDir =  Paths.get(baseDir, "excel").toString


  //必须在DfInfos前执行一次
  val totalLines = 100
  CommonTestDataGenerator.generateTestData(binDir, csvDir, excelDir, baseDir, totalLines)

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

  var expectedHostInfo: Map[String, String] = _

  val dataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin, dataSetExcel)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      Paths.get(baseDir, relativePath).toString
    }
  }
  @BeforeAll
  def startServer(): Unit = {
    CommonTestDataGenerator.generateTestData(binDir, csvDir, excelDir, baseDir, totalLines)

  }

  @AfterAll
  def stop(): Unit = {
    CommonTestDataGenerator.cleanupTestData(baseDir)
  }
}





