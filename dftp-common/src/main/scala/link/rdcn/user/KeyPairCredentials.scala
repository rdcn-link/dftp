package link.rdcn.dacp.user

import link.rdcn.user.{AuthenticationMethod, Credentials, TokenAuth, UserPrincipal}
import link.rdcn.util.{CodecUtils, KeyBasedAuthUtils}
import org.json.JSONObject

import java.security.PublicKey
import java.util.Base64

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/17 18:31
 * @Modified By:
 */
case class KeyPairCredentials(
                           serverId: String,
                           nonce: String,
                           issueTime: Long, //签发时间
                           validTo: Long, //过期时间
                           signature: Array[Byte] // UnionServer 私钥签名
                         ) extends Credentials {
  def toJson(): JSONObject = {
    val json = new JSONObject()
    json.put("serverId", serverId)
    json.put("nonce", nonce)
    json.put("issueTime", issueTime)
    json.put("validTo", validTo)
    json.put("signature", Base64.getEncoder.encodeToString(signature))
    json
  }
}

object KeyPairCredentials {
  def fromJson(json: JSONObject): KeyPairCredentials = {
    KeyPairCredentials(
      serverId = json.getString("serverId"),
      nonce = json.getString("nonce"),
      issueTime = json.getLong("issueTime"),
      validTo = json.getLong("validTo"),
      signature = Base64.getDecoder.decode(json.getString("signature"))
    )
  }
}

case class KeyPairUserPrincipal(
                             publicKey: Option[PublicKey],
                             serverId: String,
                             nonce: String,
                             issueTime: Long, //签发时间
                             validTo: Long, //过期时间
                             signature: Array[Byte] // UnionServer 私钥签名
                           ) extends UserPrincipal {
  def checkPermission(): Boolean = {
    if (publicKey.isEmpty) false else {
      if (validTo > issueTime) {
        KeyBasedAuthUtils.verifySignature(publicKey.get, getChallenge(), signature) && System.currentTimeMillis() < validTo
      } else {
        KeyBasedAuthUtils.verifySignature(publicKey.get, getChallenge(), signature)
      }
    }
  }

  private def getChallenge(): Array[Byte] = {
    val jo = new JSONObject().put("serverId", serverId)
      .put("nonce", nonce)
      .put("issueTime", issueTime)
      .put("validTo", validTo)
    CodecUtils.encodeString(jo.toString)
  }
}
