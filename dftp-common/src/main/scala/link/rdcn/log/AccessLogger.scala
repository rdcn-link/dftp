package link.rdcn.log

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/11 17:31
 * @Modified By:
 */
trait AccessLogger {

  /**
   * 记录成功请求
   * @param clientIp 客户端IP
   * @param user 认证用户
   * @param timestamp 请求时间
   * @param method 请求方法(do action/put / get stream等)
   * @param path 请求路径
   * @param status 状态码
   * @param responseSize 响应大小(字节)
   * @param durationMs 处理耗时(毫秒)
   * @param userAgent 用户代理(可选)
   */
  def logAccess(
                 clientIp: String,
                 user: String,
                 timestamp: String,
                 method: String,
                 path: String,
                 status: Int,
                 responseSize: Long,
                 durationMs: Long,
                 userAgent: Option[String] = None
               ): Unit

  /**
   * 记录失败请求
   * @param clientIp 客户端IP
   * @param user 认证用户
   * @param timestamp 请求时间
   * @param method 请求方法
   * @param path 请求路径
   * @param status 状态码
   * @param errorMessage 错误信息
   * @param durationMs 处理耗时(毫秒)
   * @param userAgent 用户代理(可选)
   */
  def logError(
                clientIp: String,
                user: String,
                timestamp: String,
                method: String,
                path: String,
                status: Int,
                errorMessage: String,
                durationMs: Long,
                userAgent: Option[String] = None
              ): Unit

  /**
   * 关闭日志记录器
   */
  def close(): Unit

}
