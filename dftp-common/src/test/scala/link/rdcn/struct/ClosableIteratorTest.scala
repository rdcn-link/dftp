/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:52
 * @Modified By:
 */
package link.rdcn.struct
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.AtomicInteger

class ClosableIteratorJunitTest {
  private val data = List(1, 2, 3)

  @Test
  def testExplicitClose_Success(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, false)

    iterator.close()

    assertEquals(1, tracker.count.get(), "Explicit close should trigger onClose callback once")
    assertTrue(!iterator.hasNext, "Iterator internal state should be marked as closed")
  }

  @Test
  def testExplicitClose_OnlyTriggersOnce(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, false)

    // 第一次关闭
    iterator.close()
    // 第二次关闭 (应该被 if (!closed) 阻止)
    iterator.close()

    assertEquals(1, tracker.count.get(), "Explicit close should only trigger onClose callback once")
  }

  @Test
  def testExplicitClose_HandlesExceptionInOnClose(): Unit = {
    // 覆盖 close() 方法中的 catch 块
    val exceptionCallback: () => Unit = () => throw new RuntimeException("Cleanup failed")
    val iterator = ClosableIterator(data.iterator, exceptionCallback, false)

    // 预期抛出包含原始消息的包装异常
    val exception = assertThrows(
      classOf[Exception],
      () => iterator.close()
    )

    assertTrue(!iterator.hasNext, "Iterator should be marked closed even if onClose throws exception")
    assertEquals("[ClosableIterator] Error during close: Cleanup failed", exception.getMessage, "Exception message should contain the original error")
  }

  @Test
  def testAutoClose_OnHasNextCompletion(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, false)

    // 消费到最后一个元素前
    iterator.next()
    iterator.next()
    iterator.next()

    // 触发 hasNext，但底层迭代器已耗尽 (覆盖 hasNext 中的自动关闭)
    val hasMore = iterator.hasNext

    assertTrue(!hasMore, "hasNext should return false after consuming all elements")
    assertEquals(1, tracker.count.get(), "onClose should be triggered when hasNext detects depletion")
  }

  @Test
  def testAutoClose_OnNextCompletion(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(List(1).iterator, tracker.callback, false)

    // 消费唯一的元素 (覆盖 next 中的自动关闭)
    iterator.next()

    assertEquals(1, tracker.count.get(), "onClose should be triggered when next() consumes the last element")
    assertTrue(!iterator.hasNext, "Iterator should be closed after last element consumed")

    // 尝试再次调用 next()，预期抛出异常
    val exception = assertThrows(
      classOf[NoSuchElementException],
      () => iterator.next()
    )
    assertEquals("next on empty iterator", exception.getMessage, "Should throw NoSuchElementException after close")
  }

  @Test
  def testNoAutoClose_IfExplicitlyClosedDuringIteration(): Unit = {
    val tracker = new CloseTracker()
    val iter = List(1, 2).iterator
    val iterator = ClosableIterator(iter, tracker.callback, false)

    iterator.next()
    iterator.close() // 显式关闭

    // 再次调用 next()，预期失败，但 onClose 只触发一次
    val exception = assertThrows(
      classOf[NoSuchElementException],
      () => iterator.next() // 触发 hasNext/next 的 closed = true 检查
    )

    assertEquals(1, tracker.count.get(), "Explicit close should prevent auto-close from triggering twice")
  }

  @Test
  def testFileListMode_NoAutoClose(): Unit = {
    val tracker = new CloseTracker()
    val iterator = ClosableIterator(data.iterator, tracker.callback, true) // isFileList = true

    // 消费所有元素
    while (iterator.hasNext) {
      iterator.next()
    }

    assertEquals(0, tracker.count.get(), "onClose should NOT be triggered automatically in FileList mode")
    // 显式关闭
    iterator.close()
    assertEquals(1, tracker.count.get(), "Explicit close should still work in FileList mode")
  }

  @Test
  def testNextOnEmptyIterator_FileListMode(): Unit = {
    val tracker = new CloseTracker()
    val emptyIterator = ClosableIterator(List.empty[Int].iterator, tracker.callback, true)

    // 在 FileList 模式下，允许对空的或耗尽的迭代器调用 next()
    // 但如果底层迭代器确实耗尽，next() 应该抛出 NoSuchElementException (这是底层 Iterator 的行为)

    assertTrue(!emptyIterator.hasNext, "hasNext should be false for empty iterator")

    // next() 抛出异常，因为底层 List.empty.iterator.next() 抛出
    val exception = assertThrows(
      classOf[NoSuchElementException],
      () => emptyIterator.next()
    )

    // 验证 next() 中没有触发 close
    assertEquals(0, tracker.count.get(), "onClose should not be triggered during next() in FileList mode")
  }
}

class CloseTracker {
  val count = new AtomicInteger(0)
  val callback: () => Unit = () => count.incrementAndGet()
}