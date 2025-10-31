package link.rdcn.client

import link.rdcn.user.{TokenAuth, UsernamePassword}
import org.json.JSONObject

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.Base64


/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/10 14:41
 * @Modified By:
 */
object AuthPlatform {

  private val clientToken: Option[String] = Some("dacp-client"+":"+"819aa4e4f4bf256602a67a61cff984f5")

  def authenticate(usernamePassword: UsernamePassword): TokenAuth = {
    val basic = "Basic " + Base64.getUrlEncoder().encodeToString(clientToken.get.getBytes())

    val paramMap = new JSONObject().put("username", "faird-user1")
      .put("password", usernamePassword.username)
      .put("grantType", usernamePassword.password)

    val httpClient = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.opendatachain.cn/auth/oauth/token"))
      .header("Authorization", basic)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(paramMap.toString()))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    TokenAuth(new JSONObject(response.body()).getString("data"))
  }

  def registerClient(clientId: String): String = {
    val paramMap = new JSONObject()
      .put("clientId", clientId)
      .put("clientName", "dacp客户端")
    val httpClient = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.opendatachain.cn/auth/client.save"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(paramMap.toString()))
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    new JSONObject(response.body()).getJSONObject("data").getString("clientSecret")
  }
}
