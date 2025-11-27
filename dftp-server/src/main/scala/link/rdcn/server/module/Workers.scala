package link.rdcn.server.module

import scala.collection.mutable.ArrayBuffer

trait TaskRunner[Worker, Result] {
  def isReady(worker: Worker): Boolean
  def executeWith(worker: Worker): Result
  def handleFailure(): Result
}

class Workers[Worker] {
  private val _list = ArrayBuffer[Worker]()

  def add(worker: Worker): Unit = _list += worker

  /** @deprecated !!! */
  def invoke[Y](runMethod: (Worker) => Y, onNull: => Y): Y = {
    _list.iterator
      .map { worker =>
        try {
          runMethod(worker)
        } catch {
          case _: MatchError => null.asInstanceOf[Y]
        }
      }
      .find(_ != null)
      .getOrElse(onNull)
//    throw new Exception(s"using work() instead!! ")
  }

  def work[Result](runner: TaskRunner[Worker, Result]): Result = {
    _list.find { worker =>
      try {
        runner.isReady(worker)
      }
      catch {
        //not covered by accept() match...cases
        case e: MatchError => false
      }
    }.map {
      worker =>
        try {
          runner.executeWith(worker)
        }
        catch {
          //not covered by run() match...cases
          case e: MatchError => runner.handleFailure()
        }
    }.getOrElse(runner.handleFailure())
  }
}

trait FilterChain[Argument, Result] {
  def doFilter(args: Argument): Result
}

trait FilterRunner[Filter, Argument, Result] {
  def doFilter(filter: Filter, args: Argument, chain: FilterChain[Argument, Result]): Result
  def startChain(chain: FilterChain[Argument, Result]): Result
}

class Filters[Filter] {
  private val _filters = ArrayBuffer[(Int, Filter)]()

  def add(order: Int, f: Filter): Unit = _filters += order -> f

  def doFilter[Argument, Result](filterRunner: FilterRunner[Filter, Argument, Result], runCore: (Argument) => Result): Result = {
    //sort by order, from low to upper
    val iterBoostlets = _filters.sortBy(_._1).map(_._2).iterator

    def step(it: Iterator[Filter]): FilterChain[Argument, Result] = new FilterChain[Argument, Result] {
      override def doFilter(args: Argument): Result = {
        if (it.hasNext) {
          filterRunner.doFilter(it.next(), args, step(it))
        }
        else {
          //core method call
          runCore(args)
        }
      }
    }

    filterRunner.startChain(step(iterBoostlets))
  }
}