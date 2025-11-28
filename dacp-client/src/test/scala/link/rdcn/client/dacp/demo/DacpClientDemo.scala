package link.rdcn.client.dacp.demo

import cn.cnic.operatordownload.client.OperatorClient
import link.rdcn.client.dacp.demo.DacpClientDemo._
import link.rdcn.client.{DacpClient, UrlValidator}
import link.rdcn.dacp.catalog.{CatalogService, CatalogServiceModule, CatalogServiceRequest, DacpCatalogModule}
import link.rdcn.dacp.cook.DacpCookModule
import link.rdcn.dacp.optree.RepositoryClient
import link.rdcn.dacp.optree.fifo.{DockerContainer, FileType}
import link.rdcn.dacp.recipe._
import link.rdcn.dacp.user.{DataOperationType, PermissionService, PermissionServiceModule}
import link.rdcn.server.ServerContext
import link.rdcn.server.module.{BaseDftpModule, DataFrameProviderService, UserPasswordAuthModule}
import link.rdcn.server.{DftpServer, DftpServerConfig}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct._
import link.rdcn.user._
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}
import org.apache.jena.vocabulary.RDF
import org.json.{JSONArray, JSONObject}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.apache.commons.io.FileUtils

object DacpClientDemo {

  var server: DftpServer = _
  val catalogService = new CatalogService {

    override def listDataSetNames(): List[String] = List("DataSet")

    override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
      val model = ModelFactory.createDefaultModel()

      val ns = "http://example.org/data/"
      model.setNsPrefix("ex", ns)

      val datasetResource: Resource = model.createResource(s"${ns}dataset/$dataSetId": String)
      val personResource: Resource = model.createResource(s"${ns}person/123": String)


      datasetResource.addProperty(model.createProperty(ns, "hasName"), "示例数据集")
        .addProperty(model.createProperty(ns, "createdBy"), personResource)

      personResource.addProperty(RDF.`type`, model.createResource(s"${ns}Person"))
        .addProperty(model.createProperty(ns, "name"), "张三")

      rdfModel.add(model)
    }

    override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
      val model = ModelFactory.createDefaultModel()

      val ns = "http://example.org/data/"
      model.setNsPrefix("ex", ns)

      val datasetResource = model.createResource(s"${ns}DataFrameName/$dataFrameName")
      val personResource = model.createResource(s"${ns}person/123")

      datasetResource.addProperty(model.createProperty(ns, "hasName"), "示例数据集")
        .addProperty(model.createProperty(ns, "createdBy"), personResource)

      personResource.addProperty(RDF.`type`, model.createResource(s"${ns}Person"))
        .addProperty(model.createProperty(ns, "name"), "张三")

      rdfModel.add(model)
    }

    override def listDataFrameNames(dataSetId: String): List[String] = List("DataFrame")

    override def getDocument(dataFrameName: String): DataFrameDocument = {
      new DataFrameDocument {
        override def getSchemaURL(): Option[String] = Some(s"$dataFrameName url")

        override def getDataFrameTitle(): Option[String] = Some(s"$dataFrameName title")

        override def getColumnURL(colName: String): Option[String] = Some(s"$dataFrameName $colName url")

        override def getColumnAlias(colName: String): Option[String] = Some(s"$dataFrameName $colName alias")

        override def getColumnTitle(colName: String): Option[String] = Some(s"$dataFrameName $colName title")
      }
    }

    override def getStatistics(dataFrameName: String): DataFrameStatistics = new DataFrameStatistics {
      override def rowCount: Long = -1L

      override def byteSize: Long = -1L
    }

    override def getSchema(dataFrameName: String): Option[StructType] =
      Some(StructType.empty.add("col1", StringType))

    override def getDataFrameTitle(dataFrameName: String): Option[String] = Some(dataFrameName)

    override def accepts(request: CatalogServiceRequest): Boolean = true
  }
  val permissionService = new PermissionService {
    override def accepts(user: UserPrincipal): Boolean = true

    override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean =
      user.asInstanceOf[UserPrincipalWithCredentials].credentials match {
        case Credentials.ANONYMOUS => false
        case UsernamePassword(username, password) => true
      }
  }

  val dataFrameProviderService = new DataFrameProviderService {
    override def accepts(dataFrameUrl: String): Boolean = true

    override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame = {
      dataFrameUrl match {
        //      op4/geo_entropy.csv
        case url if UrlValidator.extractPath(url) == "/geo_entropy.csv" =>
          val csvFile = new java.io.File("/data2/work/ncdc/faird/temp/op4/geo_entropy.csv")
          DataStreamSource.csv(csvFile, Some(","), false).dataFrame
        case url if UrlValidator.extractPath(url) == "/2019年中国榆林市沟道信息.csv" =>
          val csvFile = new java.io.File("/data2/work/ncdc/faird/temp/op1/2019年中国榆林市沟道信息.csv")
          DataStreamSource.csv(csvFile, Some(","), false).dataFrame
        case url if UrlValidator.extractPath(url) == "/2019年中国榆林市30m数字高程数据集.tif" =>
          val tifFile = new java.io.File("/data2/work/ncdc/faird/temp/op1/2019年中国榆林市30m数字高程数据集.tif")
          DataFrame.fromSeq(Seq(Blob.fromFile(tifFile)))
        case url if UrlValidator.extractPath(url) == "/csv/data_1.csv" =>
          val csvFile = new java.io.File("/data2/work/ncdc/faird/dftp-dacp/dftp-client/src/test/resources/data/csv/data_1.csv")
          DataStreamSource.csv(csvFile, Some(","), true).dataFrame
        case url if UrlValidator.extractPath(url) == "/op2/labels" =>
          val dirFile = new java.io.File("/data2/work/ncdc/faird/temp/op2/labels")
          DataStreamSource.filePath(dirFile).dataFrame
        case url if UrlValidator.extractPath(url) == "/op2/tfw" =>
          val dirFile = new java.io.File("/data2/work/ncdc/faird/temp/op2/tfw")
          DataStreamSource.filePath(dirFile).dataFrame
        case _ => new DataStreamSource {
          override def rowCount: Long = -1L

          override def schema: StructType = StructType.empty.add("col1", StringType)

          override def iterator: ClosableIterator[Row] =
            ClosableIterator(Seq.range(0, 10).map(index => Row.fromSeq(Seq("id" + index))).toIterator)()
        }.dataFrame
      }
    }
  }

  val userPasswordAuthService = new UserPasswordAuthService {
    override def authenticate(credentials: Credentials): UserPrincipal =
      UserPrincipalWithCredentials(credentials)

    override def accepts(credentials: Credentials): Boolean = true
  }

  val operatorClient = new RepositoryClient("http://10.0.89.39", 8090)
  val operatorDir = Paths.get(getClass.getClassLoader.getResource("").toURI).toString

  /**
   *
   * @param resourceName
   * @return test下名为resourceName的文件夹
   */
  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName))) // 先到test-classes中查找，然后到classes中查找
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    val nativePath: Path = Paths.get(url.toURI())
    nativePath.toString
  }

  @BeforeAll
  def startServer(): Unit = {
    val modules = Array(
      new BaseDftpModule,
      new DacpCookModule,
      new DacpCatalogModule,
      new DataFrameProviderModule(dataFrameProviderService),
      new CatalogServiceModule(catalogService),
      new UserPasswordAuthModule(userPasswordAuthService),
      new PermissionServiceModule(permissionService)
    )
    server = DftpServer.start(DftpServerConfig("0.0.0.0", 3103).withProtocolScheme("dacp"), modules)
  }

  @AfterAll
  def closeServer(): Unit = {
    server.close()
  }

}

class DacpClientDemo {

  @Test
  def catalogModuleTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102")
    println("数据帧列表-----------------")
    dc.listDataFrameNames("DataSet").foreach(println)
    println("数据集列表-----------------")
    dc.listDataSetNames().foreach(println)
    println("数据帧元数据信息-----------------")
    println(dc.getDataFrameMetaData("/abc"))
    println("数据集元数据信息-----------------")
    println(dc.getDataSetMetaData("ds"))
    println("getSchema-------------")
    println(dc.getSchema("/abc"))

    println(dc.getDocument("/abc"))
    println(dc.getStatistics("/abc"))

  }

  @Test
  def listCatalogTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102", UsernamePassword("admin", "admin"))
    val dataSets = dc.get("dacp://0.0.0.0:3102/listDataSets")
    dataSets.foreach(println)
    val dataFrames = dc.get("dacp://0.0.0.0:3102/listDataFrames/dataset")
    dataFrames.foreach(println)
    val hostInfos = dc.get("dacp://0.0.0.0:3102/listHosts")
    hostInfos.foreach(println)
  }


  @Test
  def getTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102", UsernamePassword("admin", "admin"))
    val df = dc.get("dacp://0.0.0.0:3102/DataFrame")
    df.foreach(println)
  }

  @Test
  def cookTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3102", UsernamePassword("admin", "admin"))

    val udf = new Transformer11 {
      override def transform(dataFrame: DataFrame): DataFrame = {
        dataFrame.limit(5)
      }
    }

    val transformerDAG = Flow(
      Map(
        "A" -> SourceNode("/abc"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: ExecutionResult = dc.cook(transformerDAG)
    dfs.single().foreach(println)
  }

  @Test
  def damFlowSourceGet(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))
    dc.get("dacp://0.0.0.0:3103/2019年中国榆林市沟道信息.csv").foreach(println)
    dc.get("dacp://0.0.0.0:3103/2019年中国榆林市30m数字高程数据集.tif").foreach(println)
    dc.get("dacp://0.0.0.0:3103/op2/labels").foreach(println)
    dc.get("dacp://0.0.0.0:3103/op2/tfw").foreach(println)
  }

  @Test
  def testDirectory(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val fileRepositoryGeoTransMain = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/geotrans_main.py"),
      Seq(("/data2/work/ncdc/faird/temp/op2/tfw_new", FileType.DIRECTORY),
        ("/data2/work/ncdc/faird/temp/op2/labels_new", FileType.DIRECTORY)),
      Seq(("/data2/work/ncdc/faird/temp/temp2/output", FileType.DIRECTORY)),
      DockerContainer("jyg-container")
    )

    val fileRepositorySelect = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/overlap_dam_select.py"),
      Seq(("/data2/work/ncdc/faird/temp/temp2/output1", FileType.DIRECTORY)),
      Seq(("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv", FileType.FIFO_BUFFER)),
      DockerContainer("jyg-container"))

    val recipe = Flow(
      Map(
        "sourceLabelsDir" -> SourceNode("dacp://0.0.0.0:3103/op2/labels"),
        "sourceTfwDir" -> SourceNode("dacp://0.0.0.0:3103/op2/tfw"),
        "fileRepositoryGeoTransMain" -> fileRepositoryGeoTransMain,
        "fileRepositorySelect" -> fileRepositorySelect,
        "fifoNode" -> FifoFileFlowNode()
      ),
      Map(
        "sourceLabelsDir" -> Seq("fileRepositoryGeoTransMain"),
        "sourceTfwDir" -> Seq("fileRepositoryGeoTransMain"),
        "fileRepositoryGeoTransMain" -> Seq("fileRepositorySelect"),
        "fileRepositorySelect" -> Seq("fifoNode")
        //        "fileRepositoryGeoTransMain" -> Seq("fifoNode")
      ))
    val result = dc.cook(recipe)
    result.single().foreach(println)
  }

  import java.nio.file.Paths
  import java.util.UUID
  import java.util.concurrent.atomic.AtomicLong

  @Test
  def repositoryTest(): Unit = {
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    // gully_slop 0.5.0-20251115-1
    //hydro_susceptibility  0.5.0-20251115-1
    //    val opName = "geotrans"
    //    val opVersion =  "0.5.0-20251115-2"
    val opName = "overlap_dam_select"
    val opVersion = "0.5.0-20251115-1"

    val operatorInfo = new JSONObject(client.getOperatorByNameAndVersion(opName, opVersion))
    val operatorImage = operatorInfo.getJSONObject("data").getString("nexusUrl")

    val inputCounter = new AtomicLong(0)
    val outputCounter = new AtomicLong(0)
    val ja = new JSONArray(operatorInfo.getJSONObject("data").getString("paramInfos"))
    val files = (0 until ja.length).map(index => ja.getJSONObject(index))
      //subfix,fileType,inParam,paramType
      .map(jo => (jo.getString("name"), jo.getString("fileType"), jo.getString("paramDescription"), jo.getString("paramType")))
      .map(file => {
        if (file._4 == "INPUT_FILE") {
          (file._1, file._2, file._3, file._4, s"input${inputCounter.incrementAndGet()}${file._1}")
        } else {
          (file._1, file._2, file._3, file._4, s"output${outputCounter.incrementAndGet()}${file._1}")
        }
      })
    val commands = operatorInfo.getJSONObject("data").getString("command").split(" ")

    val nodeGullySlopId = s"${opName}_${UUID.randomUUID().toString}"
    val hostPath = FileUtils.getTempDirectory("", nodeGullySlopId)
    val containerPath = s"/$nodeGullySlopId"

    val commandsWithParams = commands ++ files.flatMap(file => Seq(file._3, Paths.get(containerPath, file._5).toString))

    val inputFiles = files.filter(_._4 == "INPUT_FILE")
      .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
    val outputFiles = files.filter(_._4 == "OUTPUT_FILE")
      .map(file => (Paths.get(hostPath, file._5).toString, FileType.fromString(file._2)))
    val dockerContainer = DockerContainer(opName, Some(hostPath), Some(containerPath), Some(operatorImage))
    FifoFileBundleFlowNode(commandsWithParams, inputFiles, outputFiles, dockerContainer)
  }


  @Test
  @Disabled("该测试有害，会自动删除本地数据")
  def testDamFlowPipeline(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val opName = "gully_slop"
    val imagePath = "registry.cn-hangzhou.aliyuncs.com/cnic-piflow/siltdam-jyg-faird:2.0"


    val nodeGullySlopId = s"${opName}_${UUID.randomUUID().toString}"
    val hostPath = FileUtils.getTempDirectory("", nodeGullySlopId)
    val containerPath = s"/$nodeGullySlopId"
    val nodeGullySlop = FifoFileBundleFlowNode(
      Seq("python", "/dem/gully_slop.py", "--record_file", Paths.get(containerPath, "input1.csv").toString
        , "--dem_file", Paths.get(containerPath, "input2.tif").toString
        , "--outpath", Paths.get(containerPath, "output1.csv").toString),
      Seq((Paths.get(hostPath, "input1.csv").toString, FileType.FIFO_BUFFER),
        (Paths.get(hostPath, "input2.tif").toString, FileType.FIFO_BUFFER)),
      Seq((Paths.get(hostPath, "output1.csv").toString, FileType.FIFO_BUFFER)),
      DockerContainer(opName, Some(hostPath), Some(containerPath), Some(imagePath))
    )

    val fileRepositoryHydro = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py", "--slopfile", "/mnt/data/temp2/gully_slop_fifo_new.csv",
        "--entropyfile", "/mnt/data/temp2/geo_entropy_fifo_new.csv", "--outpath", "/mnt/data/temp2/suscep_hdyro_fifo_new12.csv"),
      Seq(("/data2/work/ncdc/faird/temp/temp2/geo_entropy_fifo_new.csv", FileType.FIFO_BUFFER),
        ("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv", FileType.FIFO_BUFFER)),
      Seq(("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo_new12.csv", FileType.FIFO_BUFFER)),
      DockerContainer("jyg-container", Some("/data2/work/ncdc/faird/temp"), Some("/mnt/data"))
    )

    val fileRepositoryGeoTransMain = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/geotrans_main.py", "--label_path", "/mnt/data/op2/labels_new",
        "--tfw_path", "/mnt/data/op2/tfw_new", "--outpath", "/mnt/data/temp2/output"),
      Seq(("/data2/work/ncdc/faird/temp/op2/tfw_new", FileType.DIRECTORY),
        ("/data2/work/ncdc/faird/temp/op2/labels_new", FileType.DIRECTORY)),
      Seq(("/data2/work/ncdc/faird/temp/temp2/output", FileType.DIRECTORY)),
      DockerContainer("jyg-container")
    )

    val fileRepositorySelect = FifoFileBundleFlowNode(
      Seq("python", "/mnt/data/temp2/overlap_dam_select.py", "--labelpath", "/mnt/data/temp2/output1",
        "--sphydrofile", "/mnt/data/temp2/suscep_hdyro_fifo_new22.csv",
        "--outpath", "/mnt/data/temp2/DamDetect_select_fifo.csv"),
      Seq(("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo_new22.csv", FileType.FIFO_BUFFER),
        ("/data2/work/ncdc/faird/temp/temp2/output1", FileType.DIRECTORY)),
      Seq(("/data2/work/ncdc/faird/temp/temp2/DamDetect_select_fifo.csv", FileType.FIFO_BUFFER)),
      DockerContainer("jyg-container")
    )

    val recipe = Flow(
      Map(
        "sourceCsv" -> SourceNode("dacp://0.0.0.0:3103/2019年中国榆林市沟道信息.csv"),
        "sourceTif" -> SourceNode("dacp://0.0.0.0:3103/2019年中国榆林市30m数字高程数据集.tif"),
        "sourceGEO" -> SourceNode("dacp://0.0.0.0:3103/geo_entropy.csv"),
        "gully" -> nodeGullySlop,
        "fileRepositoryHydro" -> fileRepositoryHydro,
        "sourceLabelsDir" -> SourceNode("dacp://0.0.0.0:3103/op2/labels"),
        "sourceTfwDir" -> SourceNode("dacp://0.0.0.0:3103/op2/tfw"),
        "fileRepositoryGeoTransMain" -> fileRepositoryGeoTransMain,
        "fileRepositorySelect" -> fileRepositorySelect,
        "fifoNode" -> FifoFileFlowNode()
      ),
      Map(
        "sourceCsv" -> Seq("gully"),
        "sourceTif" -> Seq("gully"),
        "sourceGEO" -> Seq("fileRepositoryHydro"),
        "gully" -> Seq("fileRepositoryHydro"),
        "fileRepositoryHydro" -> Seq("fileRepositorySelect"),
        "sourceTfwDir" -> Seq("fileRepositoryGeoTransMain"),
        "sourceLabelsDir" -> Seq("fileRepositoryGeoTransMain"),
        "fileRepositoryGeoTransMain" -> Seq("fileRepositorySelect"),
        "fileRepositorySelect" -> Seq("fifoNode")
      ))
    val result = dc.cook(recipe)
    result.single().foreach(println)
  }

  import cn.cnic.operatordownload.client.OperatorClient

  @Test
  def getOperatorTest(): Unit = {
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    val gullyOperatorInfo = new JSONObject(client.getOperatorByNameAndVersion("gully_slop", "0.5.0-20251115-1"))
    val hydroOperatorInfo = new JSONObject(client.getOperatorByNameAndVersion("hydro_susceptibility", "0.5.0-20251115-1"))
    val geotransOperatorInfo = new JSONObject(client.getOperatorByNameAndVersion("geotrans", "0.5.0-20251115-2"))
    val overlapOperatorInfo = new JSONObject(client.getOperatorByNameAndVersion("overlap_dam_select", "0.5.0-20251115-1"))

    println("算子信息: " + gullyOperatorInfo)
    println("算子信息: " + hydroOperatorInfo)
    println("算子信息: " + geotransOperatorInfo)
    println("算子信息: " + overlapOperatorInfo)
  }

  import link.rdcn.dacp.recipe.FlowNode

  @Test
  def vrepositoryDemoTest(): Unit = {

    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val nodeGullySlop = FlowNode.stocked("gully_slop", Some("0.5.0-20251115-1"))
    val fileRepositoryHydro = FlowNode.stocked("hydro_susceptibility", Some("0.5.0-20251115-1"))
    val fileRepositoryGeoTransMain = FlowNode.stocked("geotrans", Some("0.5.0-20251115-2"))
    val fileRepositorySelect = FlowNode.stocked("overlap_dam_select", Some("0.5.0-20251115-1"))

    val recipe = Flow(
      Map(
        "sourceCsv" -> SourceNode("dacp://0.0.0.0:3103/2019年中国榆林市沟道信息.csv"),
        "sourceTif" -> SourceNode("dacp://0.0.0.0:3103/2019年中国榆林市30m数字高程数据集.tif"),
        "sourceGEO" -> SourceNode("dacp://0.0.0.0:3103/geo_entropy.csv"),
        "gully" -> nodeGullySlop,
        "fileRepositoryHydro" -> fileRepositoryHydro,
        "sourceLabelsDir" -> SourceNode("dacp://0.0.0.0:3103/op2/labels"),
        "sourceTfwDir" -> SourceNode("dacp://0.0.0.0:3103/op2/tfw"),
        "fileRepositoryGeoTransMain" -> fileRepositoryGeoTransMain,
        "fileRepositorySelect" -> fileRepositorySelect,
        "fifoNode" -> FifoFileFlowNode()
      ),
      Map(
        "sourceCsv" -> Seq("gully"),
        "sourceTif" -> Seq("gully"),
        "sourceGEO" -> Seq("fileRepositoryHydro"),
        "gully" -> Seq("fileRepositoryHydro"),
        "fileRepositoryHydro" -> Seq("fileRepositorySelect"),
        "sourceTfwDir" -> Seq("fileRepositoryGeoTransMain"),
        "sourceLabelsDir" -> Seq("fileRepositoryGeoTransMain"),
        "fileRepositoryGeoTransMain" -> Seq("fileRepositorySelect"),
        "fileRepositorySelect" -> Seq("fifoNode")
      )
    )

    val result = dc.cook(recipe)
    result.single().foreach(println)
  }

  @Test
  def uploadPackageJarTest(): Unit = {
    val jarFile: File = new File(Paths.get(getResourcePath(""), "lib", "java").toString).listFiles().head
    val jarPath: String = jarFile.getAbsolutePath
    val functionId = "aaa.bbb.id1"
    val responseBody = operatorClient.uploadPackage(jarPath, functionId, "JAVA_JAR", "Java Application", "Transformer11", "2.0.0")
    assertTrue(Await.result(responseBody, 30.seconds).contains("success"), "Upload failed")
  }

  @Test
  def uploadPackageCppTest(): Unit = {
    val cppFile: File = new File(Paths.get(getResourcePath(""), "lib", "cpp").toString).listFiles().head
    val cppPath: String = cppFile.getAbsolutePath
    val functionId = "aaa.bbb.id2"
    val responseBody = operatorClient.uploadPackage(cppPath, functionId, "CPP_BIN", "cpp_processor_linux", "main", "3.0.0")
    assertTrue(Await.result(responseBody, 30.seconds).contains("success"), "Upload failed")
  }

  @Test
  def uploadPackagePythonTest(): Unit = {
    val pythonFile: File = new File(Paths.get(getResourcePath(""), "lib", "python").toString).listFiles().head
    val pythonPath: String = pythonFile.getAbsolutePath
    val functionId = "aaa.bbb.id3"
    val responseBody = operatorClient.uploadPackage(pythonPath, functionId, "PYTHON_BIN", "Python Application", "normalize", "3.0.0")
    assertTrue(Await.result(responseBody, 30.seconds).contains("success"), "Upload failed")
  }

  import java.nio.file.Files
  @Test
  def uploadPackageImageGullyTest(): Unit = {
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    // 上传算子
    println("=== 上传算子 ===")
    // 创建测试文件
    val op = new File("/data2/work/ncdc/faird/z_flow/gully_slop.zip")
    val requirementsFile = Files.createTempFile("requirements", ".txt")
    Files.write(requirementsFile, "numpy==1.21.0\\npandas>=1.3.0".getBytes)
    val paramFile = Files.createTempFile("param", ".json")
    Files.write(paramFile,
      """
        |[{"demo": "/path/to/input.csv", "name": ".csv", "number": 1, "fileType": "MMAP_FILE", "paramType": "INPUT_FILE", "paramDescription": "--record_file"}, {"demo": "/path/to/input.csv", "name": ".tif", "number": 1, "fileType": "MMAP_FILE", "paramType": "INPUT_FILE", "paramDescription": "--dem_file"}, {"demo": "/path/to/output.json", "name": ".csv", "number": 2, "fileType": "MMAP_FILE", "paramType": "OUTPUT_FILE", "paramDescription": "--outpath"}]
        |
        |""".stripMargin.getBytes)

    val uploadResult = client.uploadOperator(op, // file 算子文件
      requirementsFile.toFile, // requirementsFile 依赖文件
      paramFile.toFile, // paramFile 参数文件
      "gully_slop", // name 算子名称
      "0.5.0-20251127-2", // version 版本号
      "description", // description 描述
      "author", // author 作者
      "/image/testClient", // categoryPath 目录路径
      "PYTHON_IMAGE", // targetType 上传目标类型
      "python-script", // type 算子类型
      "python", // language 编程语言
      "pandas", // framework 框架
      "python /dem/gully_slop.py", // command 执行命令
      "help", // help 帮助信息
      "test@example.com", // email 邮箱
      "faird/main.py", // entryPoint 入口点
      "Test Operator" // nameEn 英文名称
    )
    println("算子上传成功: " + uploadResult)

    // 清理临时文件
    Files.delete(requirementsFile)
    Files.delete(paramFile)
  }

  import java.nio.file.Files
  @Test
  def uploadPackageImageHydroTest(): Unit = {
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    // 上传算子
    println("=== 上传算子 ===")
    // 创建测试文件
    val op = new File("/data2/work/ncdc/faird/z_flow/hydro_susceptibility.zip")
    val requirementsFile = Files.createTempFile("requirements", ".txt")
    Files.write(requirementsFile, "numpy==1.21.0\\npandas>=1.3.0".getBytes)
    val paramFile = Files.createTempFile("param", ".json")
    Files.write(paramFile,
      """
        |[{"demo": "/path/to/input.csv", "name": ".csv", "number": 1, "fileType": "MMAP_FILE", "paramType": "INPUT_FILE", "paramDescription": "--entropyfile"}, {"demo": "/path/to/input.csv", "name": ".csv", "number": 1, "fileType": "MMAP_FILE", "paramType": "INPUT_FILE", "paramDescription": "--slopfile"}, {"demo": "/path/to/output.json", "name": ".csv", "number": 2, "fileType": "MMAP_FILE", "paramType": "OUTPUT_FILE", "paramDescription": "--outpath"}]
        |
        |""".stripMargin.getBytes)

    val uploadResult = client.uploadOperator(op, // file 算子文件
      requirementsFile.toFile, // requirementsFile 依赖文件
      paramFile.toFile, // paramFile 参数文件
      "hydro_susceptibility", // name 算子名称
      "0.5.0-20251127-1", // version 版本号
      "description", // description 描述
      "author", // author 作者
      "/image/testClient", // categoryPath 目录路径
      "PYTHON_IMAGE", // targetType 上传目标类型
      "python-script", // type 算子类型
      "python", // language 编程语言
      "pandas", // framework 框架
      "python /dem/hydro_susceptibility.py", // command 执行命令
      "help", // help 帮助信息
      "test@example.com", // email 邮箱
      "faird/main.py", // entryPoint 入口点
      "Test Operator" // nameEn 英文名称
    )
    println("算子上传成功: " + uploadResult)

    // 清理临时文件
    Files.delete(requirementsFile)
    Files.delete(paramFile)
  }

  @Test
  def uploadPackageImageOverlapTest(): Unit = {
    val client: OperatorClient = OperatorClient.connect("http://10.0.89.39:8090", null)
    // 上传算子
    println("=== 上传算子 ===")
    // 创建测试文件
    val op = new File("/data2/work/ncdc/faird/z_flow/overlap_dam_select.zip")
    val requirementsFile = Files.createTempFile("requirements", ".txt")
    Files.write(requirementsFile, "numpy==1.21.0\\npandas>=1.3.0".getBytes)
    val paramFile = Files.createTempFile("param", ".json")
    Files.write(paramFile,
      """
        |[{"demo": "/path/to/input.csv", "name": ".csv", "number": 1, "fileType": "MMAP_FILE", "paramType": "INPUT_FILE", "paramDescription": "--sphydrofile"}, {"demo": "/path/to/input.csv", "name": "", "number": 1, "fileType": "DIRECTORY", "paramType": "INPUT_FILE", "paramDescription": "--labelpath"}, {"demo": "/path/to/output.json", "name": ".csv", "number": 2, "fileType": "MMAP_FILE", "paramType": "OUTPUT_FILE", "paramDescription": "--outpath"}]
        |
        |""".stripMargin.getBytes)

    val uploadResult = client.uploadOperator(op, // file 算子文件
      requirementsFile.toFile, // requirementsFile 依赖文件
      paramFile.toFile, // paramFile 参数文件
      "overlap_dam_select", // name 算子名称
      "0.5.0-20251127-1", // version 版本号
      "description", // description 描述
      "author", // author 作者
      "/image/testClient", // categoryPath 目录路径
      "PYTHON_IMAGE", // targetType 上传目标类型
      "python-script", // type 算子类型
      "python", // language 编程语言
      "pandas", // framework 框架
      "python /dem/overlap_dam_select.py", // command 执行命令
      "help", // help 帮助信息
      "test@example.com", // email 邮箱
      "faird/main.py", // entryPoint 入口点
      "Test Operator" // nameEn 英文名称
    )
    println("算子上传成功: " + uploadResult)

    // 清理临时文件
    Files.delete(requirementsFile)
    Files.delete(paramFile)
  }

  @Test
  def repositoryJarTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val sourceNode: FlowNode = FlowNode.source("/csv/data_1.csv")
    val repositoryOperator = FlowNode.stocked("aaa.bbb.id1", Some("2.0.0"))
    val recipe: Flow = Flow.pipe(sourceNode, repositoryOperator)

    val result = dc.cook(recipe)
    result.single().limit(10).foreach(println)
  }

  @Test
  def repositoryCppTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val sourceNode: FlowNode = FlowNode.source("/csv/data_1.csv")
    val repositoryOperator = FlowNode.stocked("aaa.bbb.id2", Some("3.0.0"))
    val recipe: Flow = Flow.pipe(sourceNode, repositoryOperator)

    val result = dc.cook(recipe)
    result.single().limit(10).foreach(println)
  }

  @Test
  def repositoryPythonTest(): Unit = {
    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val sourceNode: FlowNode = FlowNode.source("/csv/data_1.csv")
    val repositoryOperator = FlowNode.stocked("aaa.bbb.id3", Some("3.0.0"))
    val recipe: Flow = Flow.pipe(sourceNode, repositoryOperator)

    val result = dc.cook(recipe)
    result.single().limit(10).foreach(println)
  }


  @Test
  def cookJSONStringTest(): Unit = {
    val sourceJson: String =
      """
        |{
        |  "flow": {
        |    "paths": [
        |      {
        |        "from": "sourceCsv",
        |        "to": "gully"
        |      },
        |      {
        |        "from": "sourceTif",
        |        "to": "gully"
        |      },
        |            {
        |        "from": "sourceGEO",
        |        "to": "fileRepositoryHydro"
        |      },
        |      {
        |        "from": "gully",
        |        "to": "fileRepositoryHydro"
        |      },
        |      {
        |        "from": "fileRepositoryHydro",
        |        "to": "fileRepositorySelect"
        |      },
        |      {
        |        "from": "sourceTfwDir",
        |        "to": "fileRepositoryGeoTransMain"
        |      },
        |      {
        |        "from": "sourceLabelsDir",
        |        "to": "fileRepositoryGeoTransMain"
        |      },
        |      {
        |        "from": "fileRepositoryGeoTransMain",
        |        "to": "fileRepositorySelect"
        |      }
        |    ],
        |    "stops": [
        |      {
        |        "name": "sourceCsv",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://0.0.0.0:3103/2019年中国榆林市沟道信息.csv"
        |        }
        |      },
        |      {
        |        "name": "sourceTif",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://0.0.0.0:3103/2019年中国榆林市30m数字高程数据集.tif"
        |        }
        |      },
        |      {
        |        "name": "sourceGEO",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://0.0.0.0:3103/geo_entropy.csv"
        |        }
        |      },
        |      {
        |        "name": "sourceLabelsDir",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://0.0.0.0:3103/op2/labels"
        |        }
        |      },
        |      {
        |        "name": "sourceTfwDir",
        |        "type": "SourceNode",
        |        "properties": {
        |          "path": "dacp://0.0.0.0:3103/op2/tfw"
        |        }
        |      },
        |      {
        |        "name": "gully",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "gully_slop",
        |          "version" : "0.5.0-20251115-1"
        |        }
        |      },
        |      {
        |        "name": "fileRepositoryHydro",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "hydro_susceptibility",
        |          "version" : "0.5.0-20251115-1"
        |        }
        |      },
        |      {
        |        "name": "fileRepositoryGeoTransMain",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "geotrans",
        |          "version" : "0.5.0-20251115-2"
        |        }
        |      },
        |      {
        |        "name": "fileRepositorySelect",
        |        "type": "RepositoryNode",
        |        "properties": {
        |          "name": "overlap_dam_select",
        |          "version" : "0.5.0-20251115-1"
        |        }
        |      }
        |    ]
        |  }
        |}""".stripMargin

    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))
    val dfs: ExecutionResult = dc.cook(sourceJson)
    dfs.single().foreach(println)
  }

  @Test
  def MMAPtoFIFOMMAPTest(): Unit = {

    val dc = DacpClient.connect("dacp://0.0.0.0:3103", UsernamePassword("admin", "admin"))

    val nodeGullySlop = FlowNode.stocked("gully_slop", Some("0.5.0-20251127-2"))
    val fileRepositoryHydro = FlowNode.stocked("hydro_susceptibility", Some("0.5.0-20251127-1"))
    val fileRepositoryGeoTransMain = FlowNode.stocked("geotrans", Some("0.5.0-20251115-2"))
    val fileRepositorySelect = FlowNode.stocked("overlap_dam_select", Some("0.5.0-20251127-1"))

    val recipe = Flow(
      Map(
        "sourceCsv" -> SourceNode("dacp://0.0.0.0:3103/2019年中国榆林市沟道信息.csv"),
        "sourceTif" -> SourceNode("dacp://0.0.0.0:3103/2019年中国榆林市30m数字高程数据集.tif"),
        "sourceGEO" -> SourceNode("dacp://0.0.0.0:3103/geo_entropy.csv"),
        "gully" -> nodeGullySlop,
        "fileRepositoryHydro" -> fileRepositoryHydro,
        "sourceLabelsDir" -> SourceNode("dacp://0.0.0.0:3103/op2/labels"),
        "sourceTfwDir" -> SourceNode("dacp://0.0.0.0:3103/op2/tfw"),
        "fileRepositoryGeoTransMain" -> fileRepositoryGeoTransMain,
        "fileRepositorySelect" -> fileRepositorySelect,
        "fifoNode" -> FifoFileFlowNode()
      ),
      Map(
        "sourceCsv" -> Seq("gully"),
        "sourceTif" -> Seq("gully"),
        "sourceGEO" -> Seq("fileRepositoryHydro"),
        "gully" -> Seq("fileRepositoryHydro"),
        "fileRepositoryHydro" -> Seq("fileRepositorySelect"),
        "sourceTfwDir" -> Seq("fileRepositoryGeoTransMain"),
        "sourceLabelsDir" -> Seq("fileRepositoryGeoTransMain"),
        "fileRepositoryGeoTransMain" -> Seq("fileRepositorySelect"),
        "fileRepositorySelect" -> Seq("fifoNode")
      )
    )

    val result = dc.cook(recipe)
    result.single().foreach(println)
  }

  //  @Test
  //  def MMAPtoFIFOFIFOTest(): Unit = {
  //    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))
  //
  //    val nodeGullySlop = FifoFileBundleFlowNode(
  //      Seq("python", "/mnt/data/temp2/gully_slop.py"),
  //      Seq(""),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
  //      DockerContainer("jyg-container"),
  //      FileType.MMAP_FILE,
  //      FileType.MMAP_FILE
  //    )
  //
  //    val nodeHydro = FifoFileBundleFlowNode(
  //      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
  //      DockerContainer("jyg-container"),
  //      FileType.FIFO_BUFFER,
  //      FileType.FIFO_BUFFER
  //    )
  //
  //    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")
  //
  //    val recipe = Flow(
  //      Map(
  //        "A" -> nodeGullySlop,
  //        "B" -> nodeHydro,
  //        "C" -> fifoFileNode
  //
  //      ),
  //      Map(
  //        "A" -> Seq("B"),
  //        "B" -> Seq("C")
  //      )
  //    )
  //    val result = dacpClient.execute(recipe)
  //    result.single().foreach(println)
  //
  //  }
  //
  //  @Test
  //  def FIFOtoMMAPMMAPTest(): Unit = {
  //    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))
  //
  //    val nodeGullySlop = FifoFileBundleFlowNode(
  //      Seq("python", "/mnt/data/temp2/gully_slop.py"),
  //      Seq(""),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
  //      DockerContainer("jyg-container"),
  //      FileType.MMAP_FILE,
  //      FileType.FIFO_BUFFER
  //    )
  //
  //    val nodeHydro = FifoFileBundleFlowNode(
  //      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
  //      DockerContainer("jyg-container"),
  //      FileType.MMAP_FILE,
  //      FileType.MMAP_FILE
  //    )
  //
  //    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")
  //
  //    val recipe = Flow(
  //      Map(
  //        "A" -> nodeGullySlop,
  //        "B" -> nodeHydro,
  //        "C" -> fifoFileNode
  //
  //      ),
  //      Map(
  //        "A" -> Seq("B"),
  //        "B" -> Seq("C")
  //      )
  //    )
  //    val result = dacpClient.execute(recipe)
  //    result.single().foreach(println)
  //
  //  }
  //
  //  @Test
  //  def FIFOtoMMAPFIFOTest(): Unit = {
  //    //输出FIFO未删除
  //    val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", UsernamePassword("test", "test"))
  //
  //    val nodeGullySlop = FifoFileBundleFlowNode(
  //      Seq("python", "/mnt/data/temp2/gully_slop.py"),
  //      Seq(""),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo.csv"),
  //      DockerContainer("jyg-container"),
  //      FileType.FIFO_BUFFER,
  //      FileType.FIFO_BUFFER
  //    )
  //
  //    val nodeHydro = FifoFileBundleFlowNode(
  //      Seq("python", "/mnt/data/temp2/hydro_susceptibility.py"),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/gully_slop_fifo_new.csv"),
  //      Seq("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv"),
  //      DockerContainer("jyg-container"),
  //      FileType.MMAP_FILE,
  //      FileType.FIFO_BUFFER
  //    )
  //
  //    val fifoFileNode = FifoFileFlowNode("/data2/work/ncdc/faird/temp/temp2/suscep_hdyro_fifo.csv")
  //
  //    val recipe = Flow(
  //      Map(
  //        "A" -> nodeGullySlop,
  //        "B" -> nodeHydro,
  //        "C" -> fifoFileNode
  //
  //      ),
  //      Map(
  //        "A" -> Seq("B"),
  //        "B" -> Seq("C")
  //      )
  //    )
  //    val result = dacpClient.execute(recipe)
  //    result.single().foreach(println)
  //
  //  }

}
