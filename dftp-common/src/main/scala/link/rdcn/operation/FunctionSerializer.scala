package link.rdcn.operation

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 14:55
 * @Modified By:
 */
object FunctionSerializer {

  def serialize(obj: Serializable): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    try {
      oos.writeObject(obj)
      oos.flush()
      baos.toByteArray
    } finally {
      oos.close()
      baos.close()
    }
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    try {
      ois.readObject().asInstanceOf[T]
    } finally {
      ois.close()
      bais.close()
    }
  }
}
