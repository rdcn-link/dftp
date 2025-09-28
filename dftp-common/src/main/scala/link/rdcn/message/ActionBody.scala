package link.rdcn.server

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.nio.ByteBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/23 14:41
 * @Modified By:
 */
case class ActionBody(data: Array[Byte], params: Map[String, Any])
{
  def encode(): Array[Byte] = {
    val mapBytes = ActionBody.mapper.writeValueAsBytes(params) // Map -> JSON Bytes
    val buffer = ByteBuffer.allocate(4 + data.length + 4 + mapBytes.length)

    buffer.putInt(data.length)
    buffer.put(data)
    buffer.putInt(mapBytes.length)
    buffer.put(mapBytes)

    buffer.array()
  }
}

object ActionBody
{
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def decode(bytes: Array[Byte]): (Array[Byte], Map[String, Any]) = {
    val buffer = ByteBuffer.wrap(bytes)

    val dataLen = buffer.getInt()
    val dataBytes = new Array[Byte](dataLen)
    buffer.get(dataBytes)

    val mapLen = buffer.getInt()
    val mapBytes = new Array[Byte](mapLen)
    buffer.get(mapBytes)

    val params = mapper.readValue(mapBytes, classOf[Map[String, Any]])
    (dataBytes, params)
  }

}
