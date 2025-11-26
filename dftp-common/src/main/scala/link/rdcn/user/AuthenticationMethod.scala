package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/17 10:50
 * @Modified By:
 */
trait AuthenticationMethod {
  def accepts(credentials: Credentials): Boolean
  def authenticate(credentials: Credentials): UserPrincipal
}

trait UserPrincipal


case class UserPrincipalWithCredentials(credentials: Credentials) extends UserPrincipal

trait UserPasswordAuthService extends AuthenticationMethod { //FIXME: declare as a Class, instead of a trait
}