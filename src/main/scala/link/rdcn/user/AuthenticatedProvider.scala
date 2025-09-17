package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 10:50
 * @Modified By:
 */
trait AuthenticatedProvider {
  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   *
   */
  def authenticate(credentials: Credentials): AuthenticatedUser
}

/**
 * 用户登录状态的凭证
 */
trait AuthenticatedUser

/**
 * 登录信息作为登录状态凭证
 */
case class AuthenticatedUserWithCredentials(credentials: Credentials) extends AuthenticatedUser