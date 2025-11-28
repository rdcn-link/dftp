package link.rdcn.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import link.rdcn.dacp.user.KeyPairCredentials
import link.rdcn.user.{Credentials, TokenAuth, UsernamePassword}
import org.json.JSONObject

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 11:01
 * @Modified By:
 */
object CodecUtils {
  /** 把字符串编码成字节数组 */
  def encodeString(str: String): Array[Byte] = {
    if (str == null) Array.emptyByteArray
    else str.getBytes(StandardCharsets.UTF_8)
  }

  /** 把字节数组解码成字符串 */
  def decodeString(bytes: Array[Byte]): String = {
    if (bytes == null || bytes.isEmpty) ""
    else new String(bytes, StandardCharsets.UTF_8)
  }

  def encodePairWithTypeId(typeId: Byte, user: String, password: String): Array[Byte] = {
    val userBytes = user.getBytes(StandardCharsets.UTF_8)
    val passwordBytes = password.getBytes(StandardCharsets.UTF_8)

    val buffer: ByteBuffer = ByteBuffer.allocate(1 + 4 + userBytes.length + 4 + passwordBytes.length)
    buffer.put(typeId)
    buffer.putInt(userBytes.length)
    buffer.put(userBytes)
    buffer.putInt(passwordBytes.length)
    buffer.put(passwordBytes)

    buffer.array()
  }

  def decodePairWithTypeId(bytes: Array[Byte]): (Byte, String, String) = {
    val buffer = ByteBuffer.wrap(bytes)
    val typeId = buffer.get()
    val userLen = buffer.getInt()
    val userBytes = new Array[Byte](userLen)
    buffer.get(userBytes)
    val user = new String(userBytes, StandardCharsets.UTF_8)

    val passwordLen = buffer.getInt()
    val passwordBytes = new Array[Byte](passwordLen)
    buffer.get(passwordBytes)
    val password = new String(passwordBytes, StandardCharsets.UTF_8)

    (typeId, user, password)
  }

  def encodeCredentials(credentials: Credentials): Array[Byte] = {
    credentials match {
      case up: UsernamePassword => encodePairWithTypeId(NAME_PASSWORD, up.username, up.password)
      case token: TokenAuth => encodePairWithTypeId(TOKEN, token.token, "")
      case keypair: KeyPairCredentials => encodePairWithTypeId(KEYPAIR, keypair.toJson().toString, "")
      case Credentials.ANONYMOUS => encodePairWithTypeId(ANONYMOUS, "", "")
      case _ => throw new IllegalArgumentException(s"$credentials not supported")
    }
  }

  def decodeCredentials(bytes: Array[Byte]): Credentials = {
    lazy val result = decodePairWithTypeId(bytes)
    bytes(0) match {
      case NAME_PASSWORD => UsernamePassword(result._2, result._3)
      case TOKEN => TokenAuth(result._2)
      case ANONYMOUS => Credentials.ANONYMOUS
      case KEYPAIR => KeyPairCredentials.fromJson(new JSONObject(result._2))
      case _ => throw new IllegalArgumentException(s"${result._1} not supported")
    }
  }

  private val NAME_PASSWORD: Byte = 1
  private val TOKEN: Byte = 2
  private val ANONYMOUS: Byte = 3
  private val KEYPAIR: Byte = 4
}
