package link.rdcn.dacp.catalog

import com.sun.management.OperatingSystemMXBean
import ConfigKeys.{FAIRD_HOST_PORT, FAIRD_HOST_POSITION}
import link.rdcn.server.ServerContext
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, DataStreamSource, Row, StructType}
import link.rdcn.util.DataUtils
import org.json.{JSONArray, JSONObject}

import java.lang.management.ManagementFactory

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/30 14:12
 * @Modified By:
 */
object CatalogFormatter {

  def getDataFrameDocumentJsonString(document: DataFrameDocument,
                                             schema: Option[StructType]): String = {
    schema match {
      case None => new JSONArray().toString()
      case Some(dfSchema) =>
        val newSchema = StructType.empty
          .add("SchemaUrl",StringType)
          .add("ColumnUrl", StringType)
          .add("ColumnAlias", StringType)
          .add("ColumnTitle", StringType)
        val stream: Seq[Row] = dfSchema.columns.map(col => col.name)
          .map(name => Seq(document.getSchemaURL(),
            document.getColumnURL(name).getOrElse(""),
            document.getColumnAlias(name).getOrElse(""),
            document.getColumnTitle(name).getOrElse("")))
          .map(seq => link.rdcn.struct.Row.fromSeq(seq))
        val ja = new JSONArray()
        stream.map(_.toJsonObject(dfSchema)).foreach(ja.put(_))
        ja.toString()
    }

  }

  def getDataFrameStatisticsString(statistics: DataFrameStatistics): String = {
    val jo = new JSONObject()
    jo.put("byteSize", statistics.byteSize)
    jo.put("rowCount", statistics.rowCount)
    jo.toString()
  }

  def getHostResourceString(): String = {
    val jo = new JSONObject()
    getResourceStatusString.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }

  def getResourceStatusString(): Map[String, String] = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
    val runtime = Runtime.getRuntime

    val cpuLoadPercent = (osBean.getSystemCpuLoad * 100).formatted("%.2f")
    val availableProcessors = osBean.getAvailableProcessors

    val totalMemory = runtime.totalMemory() / 1024 / 1024 // MB
    val freeMemory = runtime.freeMemory() / 1024 / 1024 // MB
    val maxMemory = runtime.maxMemory() / 1024 / 1024 // MB
    val usedMemory = totalMemory - freeMemory

    val systemMemoryTotal = osBean.getTotalPhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryFree = osBean.getFreePhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryUsed = systemMemoryTotal - systemMemoryFree
    Map(
      "cpu.cores" -> s"$availableProcessors",
      "cpu.usage.percent" -> s"$cpuLoadPercent%",
      "jvm.memory.max.mb" -> s"$maxMemory MB",
      "jvm.memory.total.mb" -> s"$totalMemory MB",
      "jvm.memory.used.mb" -> s"$usedMemory MB",
      "jvm.memory.free.mb" -> s"$freeMemory MB",
      "system.memory.total.mb" -> s"$systemMemoryTotal MB",
      "system.memory.used.mb" -> s"$systemMemoryUsed MB",
      "system.memory.free.mb" -> s"$systemMemoryFree MB"
    )
  }

  def getHostInfoString(serverContext: ServerContext): String = {
    val hostInfo = Map(
      s"$FAIRD_HOST_POSITION" -> s"${serverContext.getHost()}",
      s"$FAIRD_HOST_PORT" -> s"${serverContext.getPort()}"
    )
    val jo = new JSONObject()
    hostInfo.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }
}
