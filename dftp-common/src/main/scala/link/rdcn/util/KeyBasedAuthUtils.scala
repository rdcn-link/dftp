package link.rdcn.util

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.json.JSONObject

import java.nio.file.{Files, Paths}
import java.security._
import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.util.Base64
import scala.collection.JavaConverters._

/**
 * @Author renhao
 * @Description:
 * @Date 2025/9/4 15:39
 * @Modified By:
 */
object KeyBasedAuthUtils {
  Security.addProvider(new BouncyCastleProvider())

  // 生成RSA密钥对
  def generateKeyPair(): KeyPair = {
    val keyGen = KeyPairGenerator.getInstance("RSA", "BC")
    keyGen.initialize(2048)
    keyGen.generateKeyPair()
  }

  def savePrivateKey(serverPrivateKey: PrivateKey, privatePath: String): Unit = {
    val privBytes = serverPrivateKey.getEncoded // PKCS#8 格式
    Files.write(Paths.get(privatePath), privBytes)
  }

  def loadPublicKey(path: String): PublicKey = {
    val pubBytes = Files.readAllBytes(Paths.get(path))
    val keySpec = new X509EncodedKeySpec(pubBytes)
    val keyFactory = KeyFactory.getInstance("RSA")
    keyFactory.generatePublic(keySpec)
  }

  def loadPrivateKey(path: String): PrivateKey = {
    val privBytes = Files.readAllBytes(Paths.get(path))
    val keySpec = new PKCS8EncodedKeySpec(privBytes)
    val keyFactory = KeyFactory.getInstance("RSA")
    keyFactory.generatePrivate(keySpec)
  }

  def savePublicKeys(map: Map[String, PublicKey], path: String): Unit = {
    val json = new JSONObject()
    map.foreach { case (key, pubKey) =>
      val base64 = Base64.getEncoder.encodeToString(pubKey.getEncoded)
      json.put(key, base64)
    }
    Files.write(Paths.get(path), json.toString(2).getBytes("UTF-8"))
  }

  def loadPublicKeys(path: String): Map[String, PublicKey] = {
    val jsonStr = new String(Files.readAllBytes(Paths.get(path)), "UTF-8")
    val json = new JSONObject(jsonStr)
    val keyFactory = KeyFactory.getInstance("RSA")
    json.keySet().asScala.map { key =>
      val base64 = json.getString(key)
      val bytes = Base64.getDecoder.decode(base64)
      val pubKey = keyFactory.generatePublic(new X509EncodedKeySpec(bytes))
      key -> pubKey
    }.toMap
  }

  // 用私钥签名 challenge
  def signData(privateKey: PrivateKey, data: Array[Byte]): Array[Byte] = {
    val signature = Signature.getInstance("SHA256withRSA", "BC")
    signature.initSign(privateKey)
    signature.update(data)
    signature.sign()
  }

  // 用公钥验证签名
  def verifySignature(publicKey: PublicKey, data: Array[Byte], sigBytes: Array[Byte]): Boolean = {
    val signature = Signature.getInstance("SHA256withRSA", "BC")
    signature.initVerify(publicKey)
    signature.update(data)
    signature.verify(sigBytes)
  }
}
