package link.rdcn.client

import link.rdcn.TestProvider
import link.rdcn.TestProvider._
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame}
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.{CallStatus, FlightRuntimeException}
import org.apache.arrow.vector.types.Types
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.File
import java.lang.invoke.CallSite

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */


class DftpClientTest extends TestProvider {

  @Test
  def testGet(): Unit = {
    val exception = assertThrows(
      classOf[FlightRuntimeException], () => dc.get("dftp://localhost:3101/csv/data_1.csv"))
    assertEquals(CallStatus.UNIMPLEMENTED.code(), exception.status().code())
    assertEquals("doGet Not Implemented", exception.status().description())
  }

  @Test
  def testPut(): Unit = {
    val dataStreamSource: DataStreamSource = DataStreamSource.filePath(new File(""))
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    val batchSize = 100
    val ackBytes:Array[Byte] = dc.put(dataFrame, batchSize)
    assertEquals(new JSONObject().put("status","success").toString, CodecUtils.decodeString(ackBytes))
  }

  @Test
  def testdoAction(): Unit = {
    val ackBytes:Array[Byte] = dc.doAction("actionName")
    assertEquals(new JSONObject().put("status","success").toString, CodecUtils.decodeString(ackBytes))  }
}
