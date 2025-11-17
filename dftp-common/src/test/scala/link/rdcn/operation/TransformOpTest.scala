/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/9 17:32
 * @Modified By:
 */
package link.rdcn.operation

import org.json.{JSONArray, JSONObject}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Collections
import scala.collection.JavaConverters.seqAsJavaListConverter

class TransformOpTest {

  // Helper functions for creating test data
  def createMapOpJson(functionJson: JSONObject, inputJson: JSONObject): String = {
    new JSONObject()
      .put("type", "Map")
      .put("function", functionJson)
      .put("input", new JSONArray(Collections.singletonList(inputJson)))
      .toString
  }

  def createSourceOpJson(dataFrameUrl: String): String = {
    new JSONObject()
      .put("type", "SourceOp")
      .put("dataFrameName", dataFrameUrl)
      .toString
  }

  // A simple function wrapper for testing
  val mockFunctionWrapper = new FunctionWrapper {
    override def applyToInput(input: Any, ctx: ExecutionContext): Any = "mock"
    override def toJson: JSONObject = new JSONObject().put("type", "JAVA_BIN").put("serializedBase64","")
  }

  val sourceOpJson: String = createSourceOpJson("dummy_url")

  // --- fromJsonString tests ---
  @Test
  def testFromJsonStringParsesSourceOp(): Unit = {
    val json = createSourceOpJson("test_source")
    val op = TransformOp.fromJsonString(json).asInstanceOf[SourceOp]
    assertEquals("test_source", op.dataFrameUrl)
  }

  @Test
  def testFromJsonStringParsesMapOp(): Unit = {
    val json = createMapOpJson(mockFunctionWrapper.toJson, new JSONObject(sourceOpJson))
    val op = TransformOp.fromJsonString(json).asInstanceOf[MapSlice]
    assertEquals("Map", op.operationType)
    assertTrue(op.inputs.head.isInstanceOf[SourceOp])
  }

  @Test
  def testFromJsonStringParsesFilterOp(): Unit = {
    val json = new JSONObject()
      .put("type", "Filter")
      .put("function", mockFunctionWrapper.toJson)
      .put("input", new JSONArray(Collections.singletonList(new JSONObject(sourceOpJson))))
      .toString
    val op = TransformOp.fromJsonString(json).asInstanceOf[FilterSlice]
    assertEquals("Filter", op.operationType)
    assertTrue(op.inputs.head.isInstanceOf[SourceOp])
  }

  @Test
  def testFromJsonStringParsesLimitOp(): Unit = {
    val json = new JSONObject()
      .put("type", "Limit")
      .put("args", new JSONArray(Collections.singletonList(10)))
      .put("input", new JSONArray(Collections.singletonList(new JSONObject(sourceOpJson))))
      .toString
    val op = TransformOp.fromJsonString(json).asInstanceOf[LimitSlice]
    assertEquals("Limit", op.operationType)
    assertEquals(10, op.n)
    assertTrue(op.inputs.head.isInstanceOf[SourceOp])
  }

  @Test
  def testFromJsonStringParsesSelectOp(): Unit = {
    val json = new JSONObject()
      .put("type", "Select")
      .put("args", new JSONArray(Seq("col1").asJava))
      .put("input", new JSONArray(Collections.singletonList(new JSONObject(sourceOpJson))))
      .toString
    val op = TransformOp.fromJsonString(json).asInstanceOf[SelectSlice]
    assertEquals("Select", op.operationType)
    assertEquals(Seq("col1"), op.columns)
    assertTrue(op.inputs.head.isInstanceOf[SourceOp])
  }
}