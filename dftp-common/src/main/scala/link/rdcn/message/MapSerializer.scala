package link.rdcn.message

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object MapSerializer {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def decodeMap(mapBytes: Array[Byte]): Map[String, Any] = {
    mapper.readValue(mapBytes, classOf[Map[String, Any]])
  }

  def encodeMap(map: Map[String, Any]): Array[Byte] = {
    mapper.writeValueAsBytes(map)
  }
}
