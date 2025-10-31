package link.rdcn.client

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/14 15:34
 * @Modified By:
 */
case class UrlValidator(protocolPrefix: String) {
  private val DftpUrlPattern = s"^${protocolPrefix}://([^:/]+)(?::(\\d+))?(/.*)?$$".r

  def validate(url: String): Either[String, (String, Option[Int], String)] = {
    url match {
      case DftpUrlPattern(host, portStr, path) =>
        val port = Option(portStr).map { p =>
          try {
            val portNum = p.toInt
            if (portNum < 0 || portNum > 65535) {
              return Left(s"Invalid port number: $portNum")
            }
            portNum
          } catch {
            case _: NumberFormatException => return Left(s"Invalid port format: $p")
          }
        }

        val normalizedPath = Option(path).getOrElse("/")
        Right((host, port, normalizedPath))

      case _ =>
        Left(s"Invalid $protocolPrefix URL format: $url. Expected format: $protocolPrefix://host[:port][/path]")
    }
  }

  def validateWithPathPrefix(url: String, requiredPrefix: String): Either[String, (String, Option[Int], String)] = {
    validate(url).flatMap { case (host, port, path) =>
      if (path.startsWith(requiredPrefix)) {
        Right((host, port, path))
      } else {
        Left(s"$protocolPrefix URL path must start with '$requiredPrefix'. Actual path: $path")
      }
    }
  }

  def validateAndExtractParam(url: String, prefix: String): Either[String, (String, Option[Int], String)] = {
    validateWithPathPrefix(url, prefix).flatMap { case (host, port, path) =>
      val param = path.stripPrefix(prefix)
      if (param.isEmpty) {
        Left(s"Missing parameter after '$prefix' in DACP URL")
      } else {
        Right((host, port, param))
      }
    }
  }

  /**
   * Quick validation check (boolean result)
   */
  def isValid(url: String): Boolean = validate(url).isRight

  /**
   * Extracts just the path component from a DFTP URL
   */
  def extractPath(url: String): Either[String, String] = {
    validate(url).map { case (_, _, path) => path }
  }
}

object UrlValidator {
  private val UrlPattern = "^([a-zA-Z][a-zA-Z0-9]*)://([^:/]+)(?::(\\d+))?(/.*)?$".r
  private val pathPattern = "^/([^/].*)?$".r

  def isPath(input: String): Boolean = {
    input match {
      case pathPattern(_*)  => true
      case _ => false
    }
  }

  def extractBase(url: String): Option[(String, String, Int)] = {
    url match {
      case UrlPattern(protocol, host, port, _) =>
        val base = if (port != null) s"$protocol://$host:$port" else s"$protocol://$host:3101"
        Some((base, host, port.toInt))
      case _ => None
    }
  }

  def validate(url: String): Either[String, (String, String, Option[Int], String)] = {
    url match {
      case UrlPattern(prefixSchema, host, portStr, path) =>
        val port = Option(portStr).map { p =>
          try {
            val portNum = p.toInt
            if (portNum < 0 || portNum > 65535) {
              return Left(s"Invalid port number: $portNum")
            }
            portNum
          } catch {
            case _: NumberFormatException => return Left(s"Invalid port format: $p")
          }
        }
        val normalizedPath = Option(path).getOrElse("/")
        Right((prefixSchema, host, port, normalizedPath))

      case _ =>
        Left(s"Invalid URL format: $url. Expected format: protocolSchema://host[:port][/path]")
    }
  }

  def extractPath(url: String): String = {
    if (isPath(url)) {
      url
    } else {
      validate(url) match {
        case Right((_, _, _, path)) => path
        case Left(error) =>
          throw new IllegalArgumentException(s"Failed to extract path from URL: $error")
      }
    }
  }

  def extractBaseUrlAndPath(url: String): Either[String, (String, String)] = {
    validate(url).map { case (protocolSchema, host, port, path) => (s"$protocolSchema://$host:${port.getOrElse(3101)}", path) }
  }
}
