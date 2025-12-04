package link.rdcn.server.exception

import link.rdcn.user.Credentials

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/4 18:31
 * @Modified By:
 */
class DftpServerException(
                           message: String,
                           cause: Throwable = null
                         ) extends RuntimeException(message, cause)

class DataFrameNotFoundException(
                                  val frameName: String,
                                  cause: Throwable = null
                                ) extends DftpServerException(s"DataFrame not found: $frameName", cause)

class DataFrameAccessDeniedException(
                                      val frameName: String,
                                      cause: Throwable = null
                                    ) extends DftpServerException(s"Access denied to DataFrame $frameName", cause)

class AuthenticationFailedException(
                                     val credential: Credentials,
                                     cause: Throwable = null
                                   ) extends DftpServerException(s"Authentication failed for: $credential", cause)
class UnknownCredentialsException(
                                   val credential: Credentials,
                                   cause: Throwable = null
                                 ) extends DftpServerException(s"unknown authentication request: $credential", cause)

class UnknownGetStreamRequestException(
                                        val token: Array[Byte],
                                        cause: Throwable = null
                                      ) extends DftpServerException(
  s"Unknown get stream request: ${token.map("%02x".format(_)).mkString(",")}",
  cause
)




