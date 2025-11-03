/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/9 17:19
 * @Modified By:
 */
package link.rdcn.operation

import link.rdcn.struct.Row
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class GenericFunctionCallTest {

  // 测试 SingleRowCall
  @Test
  def testSingleRowCallWithCorrectInput(): Unit = {
    val f = new SerializableFunction[Row, String] {
      override def apply(row: Row): String = row match {
        case v if v._1 == "test_data" => s"Processed: $v"
        case _ => "Unknown Row"
      }
    }
    val call = SingleRowCall(f)
    val input = Row("test_data")
    val result = call.transform(input)
    assertEquals("Processed: Row(test_data)", result)
  }

  @Test
  def testSingleRowCallWithIncorrectInput(): Unit = {
    val f = new SerializableFunction[Row, Any] {
      override def apply(row: Row): Any = row
    }
    val call = SingleRowCall(f)
    val incorrectInput = "This is a string" // 错误的输入类型
    var exceptionThrown = false
    try {
      call.transform(incorrectInput)
    } catch {
      case e: IllegalArgumentException =>
        exceptionThrown = true
        assertEquals("Expected Row but got class java.lang.String", e.getMessage)
    }
    assertTrue(exceptionThrown)
  }

  // 测试 RowPairCall
  @Test
  def testRowPairCallWithCorrectInput(): Unit = {
    val f = new SerializableFunction[(Row, Row), String] {
      override def apply(rows: (Row, Row)): String = s"Pair: ${rows._1}, ${rows._2}"
    }
    val call = RowPairCall(f)
    val input = (Row("row1"), Row("row2"))
    val result = call.transform(input)
    assertEquals("Pair: Row(row1), Row(row2)", result)
  }

  @Test
  def testRowPairCallWithIncorrectInput(): Unit = {
    val f = new SerializableFunction[(Row, Row), Any] {
      override def apply(rows: (Row, Row)): Any = rows
    }
    val call = RowPairCall(f)
    val incorrectInput = ("row1", "row2") // 错误的输入类型
    val exception = assertThrows(
      classOf[IllegalArgumentException], () => call.transform(incorrectInput))
    assertEquals("Expected (Row, Row) but got (row1,row2)", exception.getMessage)
  }
}
