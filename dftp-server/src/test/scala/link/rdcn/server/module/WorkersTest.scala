package link.rdcn.server.module;

import link.rdcn.operation.TransformOp
import link.rdcn.server.{DftpGetPathStreamRequest, DftpGetStreamRequest, DftpGetStreamResponse}
import link.rdcn.struct.{DataFrame, Row}
import link.rdcn.user.UserPrincipal
import org.junit.Assert
import org.junit.jupiter.api.Test;

class WorkersTest {

  class MockDftpGetPathStreamRequest(path: String) extends DftpGetPathStreamRequest {
    override def getRequestPath(): String = path

    override def getRequestURL(): String = null

    override def getTransformOp(): TransformOp = null

    override def getUserPrincipal(): UserPrincipal = null
  }

  class MockDftpGetStreamResponse extends DftpGetStreamResponse {
    var data: DataFrame = null;
    var statusCode: Int = 200

    override def sendDataFrame(data: DataFrame) {
      this.data = data
      this.statusCode = 200
    }

    def valueString() = data.collect().map(_._1.toString).reduce(_ + _)

    override def sendError(errorCode: Int, message: String) {
      statusCode = errorCode
      this.data = null
    }
  }

  @Test
  def testPerform() {
    val oa = new Workers[Int]()
    val op1 = new TaskRunner[Int, Int] {
      override def isReady(worker: Int): Boolean = true

      override def executeWith(worker: Int): Int = worker + 100

      override def handleFailure(): Int = -1
    }

    val op2 = new TaskRunner[Int, Int] {
      override def isReady(worker: Int): Boolean = worker > 50

      override def executeWith(worker: Int): Int = worker + 100

      override def handleFailure(): Int = -1
    }

    Assert.assertEquals(oa.work(op1), -1)
    oa.add(1)
    Assert.assertEquals(oa.work(op1), 101)

    oa.add(2)
    Assert.assertEquals(oa.work(op1), 101)
    Assert.assertEquals(oa.work(op2), -1)

    oa.add(200)
    Assert.assertEquals(oa.work(op2), 300)

    oa.add(400)
    Assert.assertEquals(oa.work(op2), 300)
  }

  @Test
  def testPerformWithFilter(): Unit = {
    val oa = new Workers[Int]()

    val op2 = new TaskRunner[Int, String] {
      override def isReady(worker: Int): Boolean = worker > 50

      override def executeWith(worker: Int): String = {
        s"[[$worker]]"
      }

      override def handleFailure(): String = "error"
    }

    val of1 = new FilterRunner[String, String, String] {
      override def doFilter(filter: String, args: String, chain: FilterChain[String, String]): String = {
        if (filter.contains("passby")) {
          filter
        }
        else if (filter.contains("tail")) {
          chain.doFilter(args) + "-->" + filter
        }
        else {
          filter + "-->" + chain.doFilter(args)
        }
      }

      override def startChain(chain: FilterChain[String, String]): String = chain.doFilter("unused")
    }

    oa.add(1)
    oa.add(2)
    oa.add(3)
    oa.add(51) //bingo！
    oa.add(52)

    val filters = new Filters[String]()
    filters.add(200, "f200")
    filters.add(100, "f100")
    filters.add(150, "f150(tail)")
    filters.add(300, "f300")

    Assert.assertEquals("f100-->f200-->f300-->[[51]]-->f150(tail)", filters.doFilter(of1, (args: String) => oa.work(op2)))
    filters.add(250, "f250(passby)")
    Assert.assertEquals("f100-->f200-->f250(passby)-->f150(tail)", filters.doFilter(of1, (args: String) => oa.work(op2)))
  }

  val accessControlFilter = new GetStreamFilter {
    override def doFilter(request: DftpGetStreamRequest, response: DftpGetStreamResponse, chain: GetStreamFilterChain): Unit = {
      request match {
        case r: DftpGetPathStreamRequest => {
          if (r.getRequestPath().contains("@forbidden")) {
            response.sendError(304, "access denied")
          }
          else {
            chain.doFilter(request, response)
          }
        }
      }
    }
  }

  val encryptFilter = new GetStreamFilter {
    override def doFilter(request: DftpGetStreamRequest, response: DftpGetStreamResponse, chain: GetStreamFilterChain): Unit = {
      chain.doFilter(request, response)
      request match {
        case r: DftpGetPathStreamRequest => {
          if (r.getRequestPath().contains("@private")) {
            response match {
              case r: MockDftpGetStreamResponse => {
                r.sendDataFrame(r.data.map(r => Row("***" + r._1.toString + "***")))
              }
            }
          }
        }
      }
    }
  }

  val forwardFilter = new GetStreamFilter {
    override def doFilter(request: DftpGetStreamRequest, response: DftpGetStreamResponse, chain: GetStreamFilterChain): Unit = {
      var req = request
      request match {
        case r: DftpGetPathStreamRequest => {
          if (r.getRequestPath().startsWith("/forward_a")) {
            req = new MockDftpGetPathStreamRequest("/a")
          }
        }
      }

      chain.doFilter(req, response)
    }
  }

  @Test
  def testFilteredGetStreamMethod(): Unit = {
    val method = new FilteredGetStreamMethods()
    method.addMethod(new GetStreamMethod {
      override def accepts(request: DftpGetStreamRequest): Boolean = request match {
        case r: DftpGetPathStreamRequest if r.getRequestPath().startsWith("/a") => true
      }

      override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
        response.sendDataFrame(DataFrame.fromSeq(Seq("aaaa")))
      }
    })

    method.addMethod(new GetStreamMethod {
      override def accepts(request: DftpGetStreamRequest): Boolean = request match {
        case r: DftpGetPathStreamRequest if r.getRequestPath().startsWith("/b") => true
      }

      override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
        response.sendDataFrame(DataFrame.fromSeq(Seq("bbbb")))
      }
    })

    var res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/a"), res)
    Assert.assertEquals("aaaa", res.valueString())
    Assert.assertEquals(200, res.statusCode)

    res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/b"), res)
    Assert.assertEquals("bbbb", res.valueString())
    Assert.assertEquals(200, res.statusCode)

    res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/forward_a"), res)
    Assert.assertEquals(null, res.data)
    Assert.assertEquals(404, res.statusCode)

    method.addFilter(1, accessControlFilter)
    method.addFilter(2, forwardFilter)
    method.addFilter(3, encryptFilter)

    res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/a"), res)
    Assert.assertEquals("aaaa", res.valueString())
    Assert.assertEquals(200, res.statusCode)

    res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/forward_a"), res)
    Assert.assertEquals("aaaa", res.valueString())
    Assert.assertEquals(200, res.statusCode)

    res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/a@forbidden"), res)
    Assert.assertEquals(null, res.data)
    Assert.assertEquals(304, res.statusCode)

    res = new MockDftpGetStreamResponse()
    method.handle(new MockDftpGetPathStreamRequest("/a@private"), res)
    Assert.assertEquals("***aaaa***", res.valueString())
    Assert.assertEquals(200, res.statusCode)
  }
}
