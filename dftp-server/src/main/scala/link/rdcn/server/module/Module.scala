package link.rdcn.server.module

import link.rdcn.Logging
import link.rdcn.struct.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @Author bluejoe2008
 * @Description:
 * @Data 2025/10/25 11:20
 * @Modified By:
 */
trait Module {
  def init(serverContext: ServerContext): Unit

  def phrase(): Phrase

  def service(request: DftpRequest, response: DftpResponse, next: Next): Unit
}

sealed trait Phrase

object Phrase {
  //all phrases should be ordered
  def ordered() = Array(PHRASE_AUTH, PHRASE_CONTENT, PHRASE_LOG)

  val PHRASE_LOG = new Phrase {}
  val PHRASE_AUTH = new Phrase {}
  val PHRASE_CONTENT = new Phrase {}
}

trait ServerContext {

}

trait Next {
  def service(request: DftpRequest, response: DftpResponse)
}

class ChainedModule(module: Module) {
  private var next: ChainedModule = null

  def setNext(next: ChainedModule) =
    this.next = next;

  def service(request: DftpRequest, response: DftpResponse): Unit = {
    module.service(request, response, new Next {
      override def service(request: DftpRequest, response: DftpResponse) = {
        if (next != null)
          next.service(request, response)
      }
    })
  }
}

class ModuleChain(modules: Iterable[Module]) {
  private val chainedModules = modules.map(new ChainedModule(_)).toArray
  chainedModules.sliding(2).foreach {
    case Array(a, b) => a.setNext(b)
    case _ =>
  }

  def head() = chainedModules.head
}

class Modules extends Logging {
  val modules = ArrayBuffer[Module]()
  val phrasedChainHeads = ArrayBuffer[ChainedModule]()

  def addModule(module: Module) {
    modules += module
  }

  def init(serverContext: ServerContext): Unit = {
    phrasedChainHeads ++= Phrase.ordered().map(
      phrase => {
        new ModuleChain(modules.filter(_.phrase() == phrase)).head()
      }
    )

    modules.foreach(x => {
      x.init(serverContext)
      logger.info(s"loaded module: $x")
    })
  }

  def service(request: DftpRequest, response: DftpResponse) {
    val clonedRequest = new DftpRequest {

    }
    val clonedResponse = new DftpResponse() {
      var content: DataFrame = null

      def getStatusCode(): Int = statusCode

      private var statusCode: Int = 404

      override def sendError(errorCode: Int): Unit = statusCode = errorCode

      override def sendDataFrame(df: DataFrame): Unit = {
        statusCode = 200
        content = df
      }
    }

    phrasedChainHeads.foreach {
      _.service(request, clonedResponse)
    }

    if (clonedResponse.getStatusCode() != 200)
      response.sendError(clonedResponse.getStatusCode())
    else
      response.sendDataFrame(clonedResponse.content)
  }
}

trait DftpRequest {
  val context = mutable.Map[String, Any]()
}

trait DftpResponse {
  def getStatusCode(): Int

  def sendError(errorCode: Int): Unit

  def sendDataFrame(df: DataFrame): Unit
}


case class DftGetRequest(url: String) extends DftpRequest {

}

////////////////以下为测试代码，待删除
case class AccessFileLogModule() extends Module {

  override def init(serverContext: ServerContext): Unit = {
  }

  override def service(request: DftpRequest, response: DftpResponse, next: Next): Unit = {
    next.service(request, response)
    println("logging...")
  }

  override def phrase(): Phrase = Phrase.PHRASE_LOG
}

case class AuthenticationModule() extends Module {

  override def init(serverContext: ServerContext): Unit = {
  }

  override def service(request: DftpRequest, response: DftpResponse, next: Next): Unit = {
    println("extract user principal info from request...")
    next.service(request, response)
  }

  override def phrase(): Phrase = Phrase.PHRASE_AUTH
}

case class FileDataSourceModule(prefix: String) extends Module {
  override def init(serverContext: ServerContext): Unit = {
  }

  override def service(request: DftpRequest, response: DftpResponse, next: Next): Unit = {
    request match {
      case DftGetRequest(url) if url.startsWith(s"/$prefix") =>
        response.sendDataFrame(DataFrame.fromSeq(Seq(s"${prefix}1", s"${prefix}2", s"${prefix}3")))
      case _ => next.service(request, response)
    }
  }

  override def phrase(): Phrase = Phrase.PHRASE_CONTENT
}

case class CatalogModule() extends Module {

  override def init(serverContext: ServerContext): Unit = {
  }

  override def service(request: DftpRequest, response: DftpResponse, next: Next): Unit = {
    request match {
      case DftGetRequest("/listDatasets") =>
        response.sendDataFrame(DataFrame.fromSeq(Seq("ds1", "ds2", "ds3")))
      case _ => next.service(request, response)
    }
  }

  override def phrase(): Phrase = Phrase.PHRASE_CONTENT
}

object MainTest {
  val modules = new Modules()
  modules.addModule(new AuthenticationModule)
  modules.addModule(new CatalogModule)
  modules.addModule(new FileDataSourceModule("abc"))
  modules.addModule(new AccessFileLogModule)
  modules.addModule(new FileDataSourceModule("xyz"))

  modules.init(new ServerContext {})

  def request(request: DftpRequest): Unit = {
    println()
    println(s"request from user: $request")
    modules.service(request, new DftpResponse {
      def getStatusCode(): Int = statusCode

      private var statusCode: Int = 404

      override def sendError(errorCode: Int): Unit = {
        println(s"error: $errorCode")
      }

      override def sendDataFrame(df: DataFrame): Unit = {
        println(df.schema)
        println(df.collect())
      }
    })
  }

  def main(args: Array[String]): Unit = {
    request(new DftGetRequest("/abc"))
    request(new DftGetRequest("/listDatasets"))
    request(new DftGetRequest("/404"))
    request(new DftGetRequest("/xyz"))
  }
}