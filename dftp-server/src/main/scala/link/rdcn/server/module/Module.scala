package link.rdcn.server.module

import link.rdcn.provider.DataProvider
import link.rdcn.server.{ActionRequest, ActionResponse, DftpRequest, DftpResponse, GetRequest, GetResponse, PutRequest, PutResponse}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/24 18:14
 * @Modified By:
 */
trait Module {

  def init(): Unit

  def next(): Option[Module]

  def doGet(request: GetRequest, response: GetResponse): Unit

  def doPut(request: PutRequest, putResponse: PutResponse): Unit

  def doAction(request: ActionRequest, response: ActionResponse): Unit

}

trait ModuleChainContext{}

object Module {
  def add(module: Module): Module = null
}

class DataModule(dataProvider: DataProvider) extends Module {

  override def init(): Unit = {}

  override def next(): Option[Module] = None

  override def doGet(request: GetRequest, response: GetResponse): Unit = ???

  override def doPut(request: PutRequest, putResponse: PutResponse): Unit = ???

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = ???
}

class DftpBaseModule() extends Module {
  // 依赖DataModule
  //  get map filter select + blob 流
  override def init(): Unit = {}

  override def next(): Option[Module] = ???

  override def doGet(request: GetRequest, response: GetResponse): Unit = ???

  override def doPut(request: PutRequest, putResponse: PutResponse): Unit = ???

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = ???
}

class CookModule() extends Module {
  // 依赖DataModule，任务协作
  override def init(): Unit = ???

  override def next(): Option[Module] = ???

  override def doGet(request: GetRequest, response: GetResponse): Unit = ???

  override def doPut(request: PutRequest, putResponse: PutResponse): Unit = ???

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = ???
}

class CatalogModule() extends Module{
  // 依赖DftpBaseModule，数据目录，将数据列表，封装成多种API
  override def init(): Unit = ???

  override def next(): Option[Module] = ???

  override def doGet(request: GetRequest, response: GetResponse): Unit = ???

  override def doPut(request: PutRequest, putResponse: PutResponse): Unit = ???

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = ???
}

class ModuleChain(val module: Module, var next: Option[ModuleChain] = None) {
  def service(request: DftpRequest, response: DftpResponse): Unit = ???

  def setNext(nextModule: ModuleChain): ModuleChain = {
    this.next = Some(nextModule)
    this
  }
}




