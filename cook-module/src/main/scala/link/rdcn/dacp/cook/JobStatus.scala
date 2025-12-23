package link.rdcn.dacp.cook

/**
 * @Author renhao
 * @Description:
 * @Data 2025/12/12 15:46
 * @Modified By:
 */
sealed trait JobStatus { def name: String }

object JobStatus {
  case object RUNNING  extends JobStatus { val name = "RUNNING" }
  case object COMPLETE extends JobStatus { val name = "COMPLETE" }
  case object FAILED   extends JobStatus { val name = "FAILED" }

  private val all: Map[String, JobStatus] =
    Seq(RUNNING, COMPLETE, FAILED).map(s => s.name -> s).toMap

  def fromString(s: String): JobStatus =
    all.getOrElse(s.toUpperCase, throw new IllegalArgumentException(s"Unknown status: $s"))
}

