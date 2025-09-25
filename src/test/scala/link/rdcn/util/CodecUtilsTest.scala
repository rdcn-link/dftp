package link.rdcn.util

import link.rdcn.client.ClientUtils
import link.rdcn.user.{Credentials, TokenAuth, UsernamePassword}
import org.apache.arrow.vector.types.Types
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets

class CodecUtilsTest {
  // 定义 CodecUtils 内部使用的常量，以便在测试中引用
  private val NAME_PASSWORD: Byte = 1
  private val TOKEN: Byte = 2
  private val ANONYMOUS: Byte = 3

  // --- 1. encodeString / decodeString Tests ---

  @Test
  def testEncodeDecodeString_Normal(): Unit = {
    val original = "Test String with UTF-8 characters: 编码"

    // 覆盖 encodeString 的非 null 分支
    val encoded = CodecUtils.encodeString(original)

    // 覆盖 decodeString 的非 null/非 empty 分支
    val decoded = CodecUtils.decodeString(encoded)

    assertEquals(original, decoded, "Encoded and decoded string must match")
  }

  @Test
  def testEncodeString_Null(): Unit = {
    // 覆盖 encodeString 的 null 分支
    val encoded = CodecUtils.encodeString(null)

    assertArrayEquals(Array.emptyByteArray, encoded, "Encoding null should return empty array")
  }

  @Test
  def testDecodeString_Null(): Unit = {
    // 覆盖 decodeString 的 null 分支
    val decoded = CodecUtils.decodeString(null)

    assertEquals("", decoded, "Decoding null should return empty string")
  }

  @Test
  def testDecodeString_Empty(): Unit = {
    // 覆盖 decodeString 的 empty 分支
    val decoded = CodecUtils.decodeString(Array.emptyByteArray)

    assertEquals("", decoded, "Decoding empty array should return empty string")
  }

  // --- 2. encodePairWithTypeId / decodePairWithTypeId Tests ---

  @Test
  def testEncodeDecodePairWithTypeId_Normal(): Unit = {
    val expectedTypeId: Byte = 10
    val expectedUser = "testuser"
    val expectedPassword = "securepassword"

    // 覆盖 encodePairWithTypeId
    val encoded = CodecUtils.encodePairWithTypeId(expectedTypeId, expectedUser, expectedPassword)

    // 覆盖 decodePairWithTypeId
    val decoded = CodecUtils.decodePairWithTypeId(encoded)

    assertEquals(expectedTypeId, decoded._1, "Type ID must match")
    assertEquals(expectedUser, decoded._2, "User must match")
    assertEquals(expectedPassword, decoded._3, "Password must match")
  }

  @Test
  def testEncodeDecodePairWithTypeId_EmptyStrings(): Unit = {
    val expectedTypeId: Byte = 20
    val expectedUser = ""
    val expectedPassword = ""

    val encoded = CodecUtils.encodePairWithTypeId(expectedTypeId, expectedUser, expectedPassword)
    val decoded = CodecUtils.decodePairWithTypeId(encoded)

    assertEquals(expectedTypeId, decoded._1, "Type ID must match")
    assertEquals(expectedUser, decoded._2, "User must be empty")
    assertEquals(expectedPassword, decoded._3, "Password must be empty")
  }

  // --- 3. encodeCredentials Tests ---

  @Test
  def testEncodeCredentials_UsernamePassword(): Unit = {
    val creds = UsernamePassword("user_a", "pass_b")
    // 覆盖 encodeCredentials 的 case up: UsernamePassword => ...
    val encoded = CodecUtils.encodeCredentials(creds)

    // 验证编码内容
    val decoded = CodecUtils.decodePairWithTypeId(encoded)

    assertEquals(NAME_PASSWORD, decoded._1, "Type ID should be NAME_PASSWORD")
    assertEquals(creds.username, decoded._2, "Username should match")
    assertEquals(creds.password, decoded._3, "Password should match")
  }

  @Test
  def testEncodeCredentials_TokenAuth(): Unit = {
    val token = TokenAuth("my_secure_token_xyz")
    // 覆盖 encodeCredentials 的 case token: TokenAuth => ...
    val encoded = CodecUtils.encodeCredentials(token)

    val decoded = CodecUtils.decodePairWithTypeId(encoded)

    assertEquals(TOKEN, decoded._1, "Type ID should be TOKEN")
    assertEquals(token.token, decoded._2, "Token should match")
    assertEquals("", decoded._3, "Password should be empty") // 验证 token 编码时 password 是空字符串
  }

  @Test
  def testEncodeCredentials_Anonymous(): Unit = {
    val creds = Credentials.ANONYMOUS
    // 覆盖 encodeCredentials 的 case Credentials.ANONYMOUS => ...
    val encoded = CodecUtils.encodeCredentials(creds)

    val decoded = CodecUtils.decodePairWithTypeId(encoded)

    assertEquals(ANONYMOUS, decoded._1, "Type ID should be ANONYMOUS")
    assertEquals("", decoded._2, "User should be empty")
    assertEquals("", decoded._3, "Password should be empty")
  }

  @Test
  def testEncodeCredentials_Unsupported(): Unit = {
    // 创建一个继承 Credentials 但未被 CodecUtils 处理的类型
    case class UnsupportedCredentials() extends Credentials
    val unsupported = UnsupportedCredentials()
    val exception = assertThrows(
      classOf[IllegalArgumentException], () => CodecUtils.encodeCredentials(unsupported))
    assertEquals(s"$unsupported not supported", exception.getMessage)

    // 覆盖 encodeCredentials 的 case _ => throw ...

  }

  // --- 4. decodeCredentials Tests ---

  @Test
  def testDecodeCredentials_UsernamePassword(): Unit = {
    val expectedUser = "user_x"
    val expectedPass = "pass_y"
    val encoded = CodecUtils.encodePairWithTypeId(NAME_PASSWORD, expectedUser, expectedPass)
    // 覆盖 decodeCredentials 的 case NAME_PASSWORD => ...
    val decoded = CodecUtils.decodeCredentials(encoded)

    assertTrue(decoded.isInstanceOf[UsernamePassword], "Decoded credential should be UsernamePassword")
    val up = decoded.asInstanceOf[UsernamePassword]
    assertEquals(expectedUser, up.username, "Username should match")
    assertEquals(expectedPass, up.password, "Password should match")
  }

  @Test
  def testDecodeCredentials_TokenAuth(): Unit = {
    val expectedToken = "token_data"
    val encoded = CodecUtils.encodePairWithTypeId(TOKEN, expectedToken, "ignored_pass")
    // 覆盖 decodeCredentials 的 case TOKEN => ...
    val decoded = CodecUtils.decodeCredentials(encoded)

    assertTrue(decoded.isInstanceOf[TokenAuth], "Decoded credential should be TokenAuth")
    val token = decoded.asInstanceOf[TokenAuth]
    assertEquals(expectedToken, token.token, "Token should match")
  }

  @Test
  def testDecodeCredentials_Anonymous(): Unit = {
    val encoded = CodecUtils.encodePairWithTypeId(ANONYMOUS, "ignored", "ignored")
    // 覆盖 decodeCredentials 的 case ANONYMOUS => ...
    val decoded = CodecUtils.decodeCredentials(encoded)

    assertTrue(decoded eq Credentials.ANONYMOUS, "Decoded credential should be ANONYMOUS")
  }

  @Test
  def testDecodeCredentials_UnsupportedTypeID(): Unit = {
    val UNKNOWN_ID: Byte = 99
    val encoded = CodecUtils.encodePairWithTypeId(UNKNOWN_ID, "u", "p")
    val exception = assertThrows(
      classOf[IllegalArgumentException], () => CodecUtils.decodeCredentials(encoded))
    assertEquals(s"${UNKNOWN_ID} not supported", exception.getMessage)

  }
}