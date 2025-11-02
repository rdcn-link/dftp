package link.rdcn.user

trait AuthProvider extends AuthenticationService {

  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   */
  def authenticate(credentials: Credentials): UserPrincipal

  /**
   * 判断用户是否具有某项权限
   *
   * @param user          已认证用户
   * @param dataFrameName 数据帧名称
   * @param opList        操作类型列表（Java List）
   * @return 是否有权限
   */
  def checkPermission(user: UserPrincipal,
                      dataFrameName: String,
                      opList: List[DataOperationType] = List.empty): Boolean
}

trait AuthProviderRequest {
  def getUserPrincipal(): UserPrincipal
}

