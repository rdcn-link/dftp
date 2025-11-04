package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 10:50
 * @Modified By:
 */
trait AuthenticationService  {
  type C <: Credentials

  def accepts(credentials: C): Boolean
  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   */
  def authenticate(credentials: C): UserPrincipal
}

/**
 * 用户登录状态的凭证
 */
trait UserPrincipal

/**
 * 登录信息作为登录状态凭证
 */
case class UserPrincipalWithCredentials(credentials: Credentials) extends UserPrincipal

trait UserPasswordAuthService extends AuthenticationService{
  override type C = UsernamePassword
}