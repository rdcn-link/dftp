package link.rdcn.dacp.recipe

import link.rdcn.struct.DataFrame

trait ExecutionResult {
  def single(): DataFrame

  def get(name: String): DataFrame

  def map(): Map[String, DataFrame]
}
