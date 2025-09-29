package link.rdcn.client

import link.rdcn.ClientTestProvider
import link.rdcn.ClientTestProvider._
import link.rdcn.ClientTestBase._
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame}
import link.rdcn.util.CodecUtils
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.Paths
import scala.io.{BufferedSource, Source}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */


class DftpClientTest extends ClientTestProvider {
  @Test
  def testGet(): Unit = {
    val source: BufferedSource = Source.fromFile(Paths.get(csvDir, "data_1.csv").toString)
    val expectedOutput = source.getLines().toSeq.tail.mkString("\n") + "\n"
    val dataFrame = dc.get("dftp://localhost:3101/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    dataFrame.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput)
    source.close()
  }

  @Test
  def testPut(): Unit = {
    val dataStreamSource: DataStreamSource = DataStreamSource.filePath(new File(""))
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    val batchSize = 100
    val ackBytes: Array[Byte] = dc.put(dataFrame, batchSize)
    assertEquals(new JSONObject().put("status", "success").toString, CodecUtils.decodeString(ackBytes))
  }

  @Test
  def testdoAction(): Unit = {
    val ackBytes: Array[Byte] = dc.doAction("actionName")
    assertEquals(new JSONObject().put("status", "success").toString, CodecUtils.decodeString(ackBytes))
  }
}
