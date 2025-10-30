package link.rdcn.optree

import link.rdcn.optree.fifo.{DockerContainer, DockerExecute, RowFilePipe}
import link.rdcn.operation.{ExecutionContext, FunctionSerializer, FunctionWrapper, GenericFunctionCall}
import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.{JSONArray, JSONObject}

import java.io.{BufferedReader, BufferedWriter, File, InputStreamReader, OutputStreamWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.Paths
import java.util.{Base64, ServiceLoader, UUID}
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter, mapAsScalaMapConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/18 11:16
 * @Modified By:
 */
trait TransformFunctionWrapper extends FunctionWrapper {
  def toJson: JSONObject

  def applyToInput(input: Any, ctx: ExecutionContext): Any = {
    require(input.isInstanceOf[Seq[DataFrame]] && ctx.isInstanceOf[FlowExecutionContext])
    applyToDataFrames(input.asInstanceOf[Seq[DataFrame]], ctx.asInstanceOf[FlowExecutionContext])
  }

  def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame
}

object TransformFunctionWrapper {
  def fromJsonObject(jo: JSONObject): TransformFunctionWrapper = {
    jo.getString("type") match {
      case LangTypeV2.PYTHON_CODE.name => PythonCode(jo.getString("code"))
      case LangTypeV2.JAVA_BIN.name => JavaBin(jo.getString("serializedBase64"))
      case LangTypeV2.JAVA_CODE.name => JavaCode(jo.getString("javaCodeString"))
      case LangTypeV2.PYTHON_BIN.name => PythonBin(jo.getString("functionName"), jo.getString("whlPath"))
      case LangTypeV2.JAVA_JAR.name => JavaJar(jo.getString("jarPath"), jo.getString("functionName"))
      case LangTypeV2.CPP_BIN.name => CppBin(jo.getString("cppPath"))
      case LangTypeV2.REPOSITORY_OPERATOR.name => RepositoryOperator(jo.getString("functionID"))
      case LangTypeV2.FILE_REPOSITORY_BUNDLE.name => {
        val command = jo.getJSONArray("command").toList.asScala.map(_.toString)
        val inputFilePath = jo.getJSONArray("inputFilePath").toList.asScala.map(_.toString)
        val outPutFilePath = jo.getJSONArray("outputFilePath").toList.asScala.map(_.toString)
        val dockerContainer = DockerContainer.fromJson(jo.getJSONObject("dockerContainer"))

        FileRepositoryBundle(command, inputFilePath, outPutFilePath, dockerContainer)
      }
    }
  }
  def getJavaSerialized(functionCall: GenericFunctionCall): JavaBin = {
    val objectBytes = FunctionSerializer.serialize(functionCall)
    val base64Str: String = java.util.Base64.getEncoder.encodeToString(objectBytes)
    JavaBin(base64Str)
  }
}

case class PythonCode(code: String, batchSize: Int = 100) extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.PYTHON_CODE.name)
    jo.put("code", code)
  }

  override def toString(): String = "PythonCodeNode Function"

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val stream = inputs.head.mapIterator[Iterator[Row]](iter => {
      new Iterator[Row] {
        private val grouped: Iterator[Seq[Row]] = iter.grouped(batchSize)

        private var currentBatchIter: Iterator[Row] = Iterator.empty

        override def hasNext: Boolean = {
          while (!currentBatchIter.hasNext && grouped.hasNext) {
            val batch = grouped.next()

            // Convert Seq[Row] => java.util.List[java.util.List[AnyRef]]
            val javaBatch = new java.util.ArrayList[java.util.List[AnyRef]]()
            for (row <- batch) {
              val rowList = new java.util.ArrayList[AnyRef]()
              row.toSeq.foreach(v => rowList.add(v.asInstanceOf[AnyRef]))
              javaBatch.add(rowList)
            }
            val interp = ctx.getSharedInterpreter().getOrElse(throw new Exception("Failed to load SharedInterpreter"))
            interp.set("input_data", javaBatch)
            interp.exec(code)
            val result = interp.getValue("output_data", classOf[java.util.List[java.util.List[AnyRef]]])
            val scalaRows = result.asScala.map(Row.fromJavaList)
            currentBatchIter = scalaRows.iterator
          }

          currentBatchIter.hasNext
        }

        override def next(): Row = {
          if (!hasNext) throw new NoSuchElementException("No more rows")
          currentBatchIter.next()
        }
      }
    })
    getDataFrameByStream(stream)
  }
}

case class JavaBin(serializedBase64: String) extends TransformFunctionWrapper {

  lazy val genericFunctionCall: GenericFunctionCall = {
    val restoredBytes = java.util.Base64.getDecoder.decode(serializedBase64)
    FunctionSerializer.deserialize(restoredBytes).asInstanceOf[GenericFunctionCall]
  }

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.JAVA_BIN.name)
    jo.put("serializedBase64", serializedBase64)
  }

  override def toString(): String = "Java_bin Function"

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    inputs.length match {
      case 1 => genericFunctionCall.transform(inputs.head).asInstanceOf[DataFrame]
      case 2 => genericFunctionCall.transform((inputs.head, inputs.last)).asInstanceOf[DataFrame]
      case other => throw new IllegalArgumentException(s"Unsupported inputs DataFrames length: $other")
    }
  }
}

case class JavaCode(javaCodeString: String) extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.JAVA_CODE.name)
    jo.put("javaCodeString", javaCodeString)
  }

  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    //TODO 支持对一组DataFrame的处理
    input.head.mapIterator[DataFrame](iter => {
      val clazzMap = FunctionSerializer.deserialize(Base64.getDecoder.decode(javaCodeString)).asInstanceOf[java.util.Map[String, Array[Byte]]]
      val classLoader = new ByteArrayClassLoader(clazzMap.asScala.toMap, Thread.currentThread().getContextClassLoader)
      val mainClassName = clazzMap.asScala.keys.find(!_.contains("$"))
        .getOrElse(throw new RuntimeException("cannot find main class name"))
      val clazz = classLoader.loadClass(mainClassName)
      val instance = clazz.getDeclaredConstructor().newInstance()
      val method = clazz.getMethod("transform", classOf[DataFrame])
      method.invoke(instance, getDataFrameByStream(iter)).asInstanceOf[DataFrame]
    })
  }

  private class ByteArrayClassLoader(classBytes: Map[String, Array[Byte]], parent: ClassLoader) extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      // 检查当前类加载器是否已经加载过这个类
      val loadedClass = findLoadedClass(name)
      if (loadedClass != null) {
        return loadedClass
      }

      // 尝试从传入的字节码 Map 中查找
      classBytes.get(name) match {
        case Some(bytes) =>
          defineClass(name, bytes, 0, bytes.length)
        case None =>
          super.findClass(name)
      }
    }
  }
}

case class PythonBin(functionName: String, whlPath: String, batchSize: Int = 100) extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.PYTHON_BIN.name)
    jo.put("functionName", functionName)
    jo.put("whlPath", whlPath)
  }
  //TODO 支持对一组DataFrame的处理
  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val jep = ctx.getSubInterpreter(Paths.get(ctx.pythonHome,
        LangTypeV2.PYTHON_BIN.name+UUID.randomUUID()).toString, whlPath)
      .getOrElse(throw new IllegalArgumentException("Python interpreter is required"))
    jep.eval("import link.rdcn.operators.registry as registry")
    jep.set("operator_name", functionName)
    jep.eval("func = registry.get_operator(operator_name)")
    val stream = input.head.mapIterator(rows => {
      rows.grouped(batchSize).flatMap(rowSeq => {
        jep.set("input_rows", rowSeq.map(_.toSeq.asJava).asJava)
        jep.eval("output_rows = func(input_rows)")
        val result = jep.getValue("output_rows").asInstanceOf[java.util.List[java.util.List[Object]]]
        result.asScala.map(Row.fromJavaList(_))
      })
    })
    DataUtils.getDataFrameByStream(stream)
  }
}

case class JavaJar(jarPath: String, functionName: String) extends TransformFunctionWrapper {
  override def toJson: JSONObject = {
    val jo = new JSONObject()
    jo.put("type", LangTypeV2.JAVA_JAR.name)
    jo.put("jarPath", jarPath)
    jo.put("functionName", functionName)
  }

  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val jarFile = new java.io.File(jarPath)
    val urls = Array(jarFile.toURI.toURL)
    val parentLoader = getClass.getClassLoader
    val pluginLoader = new PluginClassLoader(urls, parentLoader)
    functionName match {
      case "Transformer11" =>
        val serviceLoader = ServiceLoader.load(classOf[link.rdcn.recipe.Transformer11], pluginLoader).iterator()
        if (!serviceLoader.hasNext) throw new Exception(s"No Transformer11 implementation class was found in this jar $jarPath")
        serviceLoader.next().transform(input.head)
      case "Transformer21" =>
        val serviceLoader = ServiceLoader.load(classOf[link.rdcn.recipe.Transformer21], pluginLoader).iterator()
        if (!serviceLoader.hasNext) throw new Exception(s"No Transformer21 implementation class was found in this jar $jarPath")
        serviceLoader.next().transform(input.head, input.last)
      case other => throw new IllegalArgumentException(s"Unsupported input function type: $other")
    }
  }
  private class PluginClassLoader(urls: Array[URL], parent: ClassLoader)
    extends URLClassLoader(urls, parent) {

    override def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
      // 必须由父加载器加载的类（避免 LinkageError）
      if (name.startsWith("scala.") ||
        name.startsWith("link.rdcn.") // 主程序中定义的接口、DataFrame等
      ) {
        return super.loadClass(name, resolve) // 委托给 parent
      }

      try {
        val clazz = findClass(name)
        if (resolve) resolveClass(clazz)
        clazz
      } catch {
        case _: ClassNotFoundException =>
          super.loadClass(name, resolve)
      }
    }
  }
}

case class CppBin(cppPath: String) extends TransformFunctionWrapper {

  override def toJson: JSONObject = new JSONObject().put("type", LangTypeV2.CPP_BIN.name)
    .put("cppPath", cppPath)

  override def applyToDataFrames(input: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    val pb = new ProcessBuilder(cppPath)
    val process = pb.start()
    val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    val inputDataFrame = input.head
    val inputSchema = inputDataFrame.schema
    inputDataFrame.mapIterator[DataFrame](iter => {
      val stream = new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          val row = iter.next()
          val jsonStr = row.toJsonString(inputSchema)
          // 写入 stdin
          writer.write(jsonStr)
          writer.newLine()
          writer.flush()
          // 从 stdout 读取一行响应 JSON
          val response = reader.readLine()
          if (response == null) throw new RuntimeException("Unexpected end of output from C++ process.")
          response
        }
      }
      val r = DataUtils.getStructTypeStreamFromJson(stream)
      val autoClosingIterator = ClosableIterator(r._1)(() => {
        iter.close()
        writer.close()
        reader.close()
        process.destroy()
      })
      DefaultDataFrame(r._2, autoClosingIterator)
    })
  }
}

case class RepositoryOperator(functionID: String) extends TransformFunctionWrapper {

  override def toJson: JSONObject = new JSONObject().put("type", LangTypeV2.REPOSITORY_OPERATOR.name)
    .put("functionID", functionID)

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    ctx.getRepositoryClient().getOrElse(throw new Exception("Operator repository client not found. Please configure the client settings."))
      .executeOperator(functionID, inputs, ctx)
  }
}

case class FileRepositoryBundle(
                                 command: Seq[String],
                                 inputFilePath: Seq[String],
                                 outputFilePath: Seq[String],
                                 dockerContainer: DockerContainer
                               )
  extends TransformFunctionWrapper {

  override def toJson: JSONObject = {
    val jo = new JSONObject
    jo.put("type", LangTypeV2.FILE_REPOSITORY_BUNDLE.name)
    jo.put("command", new JSONArray(command.asJava))
    jo.put("inputFilePath", new JSONArray(inputFilePath.asJava))
    jo.put("outputFilePath", new JSONArray(outputFilePath.asJava))
    jo.put("dockerContainer", dockerContainer.toJson())
    jo
  }

  def deleteFiFOFile(): Unit = {
    outputFilePath.foreach(filePath => {
      Runtime.getRuntime.exec(Array("rm", "-rf", filePath))
    })
  }

  override def applyToDataFrames(inputs: Seq[DataFrame], ctx: FlowExecutionContext): DataFrame = {
    dockerContainer.start()
    //创建fifo文件
    (inputFilePath ++ outputFilePath).foreach(path=>RowFilePipe.fromFilePath(path))
    DockerExecute.nonInteractiveExec(command.toArray, dockerContainer.containerName) //"jyg-container"
    //TODO 支持输出多个DataFrame 对应多个fifo文件
    RowFilePipe(new File(outputFilePath.head)).dataFrame()
  }
}