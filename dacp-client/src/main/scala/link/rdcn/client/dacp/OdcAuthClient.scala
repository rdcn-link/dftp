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
object OdcAuthClient {

  private val clientToken: Option[String] = Some("dacp-client"+":"+"819aa4e4f4bf256602a67a61cff984f5")

  def requestAccessToken(usernamePassword: UsernamePassword): TokenAuth = {
    val basic = "Basic " + Base64.getUrlEncoder().encodeToString(clientToken.get.getBytes())

    val paramMap = new JSONObject().put("username", usernamePassword.username)
      .put("password", usernamePassword.password)
      .put("grantType", "password")

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

  def createClientCredentials(clientId: String): String = {
    val paramMap = new JSONObject()
      .put("clientId", clientId)
      .put("clientName", "dacp client")
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
