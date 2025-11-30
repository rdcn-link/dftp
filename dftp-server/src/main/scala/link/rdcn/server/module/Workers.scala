package link.rdcn.server.module

import scala.collection.mutable.ArrayBuffer

trait TaskRunner[Worker, Result] {
  def acceptedBy(worker: Worker): Boolean
  def executeWith(worker: Worker): Result
  def handleFailure(): Result
}

class Workers[Worker] {
  private val _list = ArrayBuffer[Worker]()

  def add(worker: Worker): Unit = _list += worker

  def work[Result](runMethod: (Worker) => Result, onFail: => Result, ifAccepts: (Worker) => Boolean = _ => true): Result = {
    work(new TaskRunner[Worker, Result] {
      override def acceptedBy(worker: Worker): Boolean = ifAccepts(worker)

      override def executeWith(worker: Worker): Result = runMethod(worker)

      override def handleFailure(): Result = onFail
    })
  }

  def work[Result](runner: TaskRunner[Worker, Result]): Result = {
    _list.find { worker =>
      try {
        runner.acceptedBy(worker)
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
    //sort filters in ascending order
    val iterFilters = _filters.sortBy(_._1).map(_._2).iterator

    def step(it: Iterator[Filter]): FilterChain[Argument, Result] = new FilterChain[Argument, Result] {
      override def doFilter(args: Argument): Result = {
        if (it.hasNext) {
          filterRunner.doFilter(it.next(), args, step(it))
        }
        else {
          //finish all filters, now call core method
          runCore(args)
        }
      }
    }

    filterRunner.startChain(step(iterFilters))
  }
}