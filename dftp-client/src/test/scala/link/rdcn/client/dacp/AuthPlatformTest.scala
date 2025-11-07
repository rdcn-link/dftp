/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/4 17:39
 * @Modified By:
 */
package link.rdcn.client.dacp


import link.rdcn.client.AuthPlatform
import link.rdcn.user.UsernamePassword
import org.json.JSONException
import org.junit.jupiter.api.Assertions.{assertNotNull, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.Test

import java.io.IOException

class AuthPlatformTest {

  /**
   * 测试 authenticate 方法在提供无效凭证时是否会失败.
   * 这是一个网络集成测试.
   *
   * @Disabled 解释: 这是一个集成测试, 会访问真实的网络 API.
   * 默认禁用以防止 CI/CD 失败.
   */
  @Test
//  @Disabled("这是一个集成测试, 会访问真实的网络 API. 请在需要验证网络连通性时手动启用.")
  def testAuthenticateWithInvalidCredentials(): Unit = {
    // 准备: 使用一个极不可能有效的用户名和密码
    val invalidPasswordForFairdUser1 = s"invalid_pass_${System.currentTimeMillis()}"
    val grantType = "password" // 假设

    val creds = new UsernamePassword(invalidPasswordForFairdUser1, grantType)

    // 执行并验证:
    // 期望 AuthPlatform.authenticate 失败, 因为凭证无效.
    // 失败的形式可能是:
    // - API 返回一个错误 JSON (例如: {"error": "..."}),
    // - 导致 new JSONObject(response.body()).getString("data") 抛出 JSONException.
    // - 或者是 HTTP 4xx/5xx 错误, 导致 httpClient.send() 抛出 IOException.

    val exception = assertThrows(classOf[Exception], () => {
      AuthPlatform.authenticate(creds)
      ()
    }, "使用无效凭证进行身份验证应抛出异常")

    // 验证异常消息不为空
    assertNotNull(exception.getMessage, "异常消息不应为 null")

    // 更具体地检查, 确保它是一个 JSON 解析错误
    // (因为无效凭证返回的 JSON 响应中没有 "data" 字段)
    assertTrue(exception.isInstanceOf[JSONException] || exception.isInstanceOf[IOException],
      "异常类型应为 JSONException (解析错误响应) 或 IOException (网络/HTTP错误)")
  }

  /**
   * 测试 registerClient 方法.
   *
   * @Disabled 解释: 此测试会修改外部生产 API, 默认禁用.
   * 必须手动启用以确认其危险操作.
   */
  @Test
//  @Disabled("此测试会修改外部生产 API, 默认禁用. 必须手动启用.")
  def testRegisterClient(): Unit = {
    // 准备: 创建一个唯一的客户端 ID, 以免与现有 ID 冲突
    val uniqueClientId = s"scala_test_client_${System.currentTimeMillis()}"

    var clientSecret: String = null
    try {
      clientSecret = AuthPlatform.registerClient(uniqueClientId)
    } catch {
      case e: Exception =>
        fail(s"注册客户端失败, 抛出意外异常: ${e.getMessage}", e)
    }

    // 验证
    assertNotNull(clientSecret, "客户端密钥不应为 null")
    assertTrue(clientSecret.nonEmpty, "客户端密钥不EMpty")
  }
}
