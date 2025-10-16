// file: dftp-server/src/main/scala/.../BlobTransferRegistry.scala
package link.rdcn.struct

import java.io.InputStream
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}
import scala.concurrent.{ExecutionContext, Future}

/**
 * 管理从上游到下游的 Blob 异步传输任务。
 * 这是一个线程安全的单例对象。
 */
object BlobTransferRegistry {
  // 定义一个特殊的“信号”对象，表示流已正常结束
  val END_OF_STREAM = new Array[Byte](0)

  // Key: 传输任务ID (String), Value: 数据块队列 (BlockingQueue)
  private val activeTransfers = new ConcurrentHashMap[String, BlockingQueue[Array[Byte]]]()

  /**
   * 启动一个新的 Blob 传输任务。
   * 它会在后台线程中开始从上游 Blob 拉取数据，并放入一个队列中。
   * @param blob 一个惰性的、连接到上游的 Blob 对象
   * @return 分配给这次传输的唯一任务 ID
   */
  def startTransfer(blob: Blob): String = {
    val transferId = java.util.UUID.randomUUID().toString
    // 创建一个有界的队列，例如最多缓存 16 个 64KB 的数据块（约1MB），防止内存无限增长
    val dataQueue = new LinkedBlockingQueue[Array[Byte]](16)

    activeTransfers.put(transferId, dataQueue)

    // 在一个独立的后台线程中开始“搬运”数据
    Future {
      try {
        blob.offerStream { inputStream: InputStream =>
          val buffer = new Array[Byte](65536) // 每次搬运 64KB
          var bytesRead = inputStream.read(buffer)
          while (bytesRead > 0) {
            val chunk = java.util.Arrays.copyOf(buffer, bytesRead)
            // 将数据块放入队列，如果队列满了，这个调用会自动阻塞，从而实现背压
            dataQueue.put(chunk)
            bytesRead = inputStream.read(buffer)
          }
        }
      } catch {
        case e: Throwable =>
          println(s"ERROR: Upstream transfer for transfer ID $transferId failed.")
          e.printStackTrace()
      } finally {
        // 无论成功还是失败，都必须放入“结束信号”，以唤醒可能正在等待的消费者
        dataQueue.put(END_OF_STREAM)
        println(s"DEBUG: Upstream transfer for ID $transferId finished.")
      }
    }(ExecutionContext.global) // 使用 Scala 的全局线程池

    transferId
  }

  /**
   * 获取一个正在进行的传输任务的数据队列
   */
  def getQueue(transferId: String): Option[BlockingQueue[Array[Byte]]] = {
    Option(activeTransfers.get(transferId))
  }

  /**
   * 完成并清理一个传输任务
   */
  def endTransfer(transferId: String): Unit = {
    activeTransfers.remove(transferId)
    println(s"DEBUG: Transfer task $transferId cleaned up.")
  }

  def consumeTransfer[T](transferId: String)(consume: BlockingQueue[Array[Byte]] => T): Option[T] = {
    // 使用 getQueue 安全地获取队列
    getQueue(transferId).map { dataQueue =>
      try {
        // 执行用户提供的消费函数
        consume(dataQueue)
      } finally {
        // 无论消费函数是成功还是失败，都确保清理任务
        endTransfer(transferId)
      }
    }

  }}