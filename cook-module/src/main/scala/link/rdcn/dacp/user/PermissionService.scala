package link.rdcn.dacp.user

import link.rdcn.user.UserPrincipal

trait PermissionService{

  def accepts(user: UserPrincipal): Boolean

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

