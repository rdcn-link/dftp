package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 16:55
 * @Modified By:
 */
trait Credentials extends Serializable

object AnonymousCredentials extends Credentials

object Credentials {
  val ANONYMOUS = AnonymousCredentials
}


