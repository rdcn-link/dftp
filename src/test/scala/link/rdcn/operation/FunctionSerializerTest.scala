/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:38
 * @Modified By:
 */
package link.rdcn.operation

import link.rdcn.client.ClientUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.io.Serializable
case class TestData(id: Int, name: String) extends Serializable


class FunctionSerializerTest {

  @Test
  def testSerializeAndDeserializeSuccess(): Unit = {
    val originalObject = TestData(101, "Test Function Payload")

    val serializedBytes = FunctionSerializer.serialize(originalObject)

    assertTrue(serializedBytes.length > 0, "Serialized bytes array should not be empty")

    val deserializedObject = FunctionSerializer.deserialize[TestData](serializedBytes)

    assertTrue(deserializedObject.isInstanceOf[TestData], "Deserialized object should be of type TestData")

    assertEquals(originalObject.id, deserializedObject.id, "ID field must match after roundtrip")
    assertEquals(originalObject.name, deserializedObject.name, "Name field must match after roundtrip")

    assertTrue(originalObject ne deserializedObject, "Deserialized object should be a new instance")
  }

  @Test()
  def testSerialize_NotSerializable(): Unit = {
    class NonSerializableClass(val value: Int)
    val nonSerializableObject = new NonSerializableClass(42)

    val exception = assertThrows(
      classOf[java.lang.ClassCastException], () => FunctionSerializer.serialize(nonSerializableObject.asInstanceOf[Serializable]))
  }

  @Test()
  def testDeserialize_CorruptedData(): Unit = {
    val corruptedBytes = Array[Byte](1, 2, 3, 4, 5)

    val exception = assertThrows(
      classOf[java.io.StreamCorruptedException], () => FunctionSerializer.deserialize[TestData](corruptedBytes))

  }
}