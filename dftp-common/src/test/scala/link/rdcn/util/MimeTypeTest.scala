/**
 * @Author Yomi
 * @Description:
 * @Data 2025/9/26 10:55
 * @Modified By:
 */
package link.rdcn.util

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

class MimeTypeFactoryTest {

  // --- 准备用于测试的 "Magic Numbers" (文件头字节) ---

  // PNG: 89 50 4E 47 0D 0A 1A 0A
  val PNG_MAGIC: Array[Byte] = Array(
    0x89.toByte, 0x50.toByte, 0x4E.toByte, 0x47.toByte,
    0x0D.toByte, 0x0A.toByte, 0x1A.toByte, 0x0A.toByte
  )

  // PDF: %PDF-1.
  val PDF_MAGIC: Array[Byte] = "%PDF-1.4".getBytes(StandardCharsets.US_ASCII)

  // TEXT: MimeUtil 通常会将可读的 US-ASCII 识别为 text/plain
  val TEXT_DATA: Array[Byte] = "This is a plain text file.".getBytes(StandardCharsets.US_ASCII)

  // EMPTY
  val EMPTY_DATA: Array[Byte] = Array.emptyByteArray

  // --- MimeType (Case Class) 测试 ---

  @Test
  def testMimeTypeMajorMinor(): Unit = {
    // 假设 'application/pdf' 在 properties 中
    val pdfType = MimeTypeFactory.fromText("application/pdf")

    assertEquals("application", pdfType.major, "Major type (application) 应被正确解析")
    assertEquals("pdf", pdfType.minor, "Minor type (pdf) 应被正确解析")
  }

  // --- MimeTypeFactory (Object) 测试 ---

  @Test
  def testFromText_Success(): Unit = {
    // 假设 100=image/png 在 properties 中
    val mimeType = MimeTypeFactory.fromText("image/png")
    assertEquals(278L, mimeType.code, "应根据 properties 文件找到正确的 code")
    assertEquals("image/png", mimeType.text, "Text 应保持不变 (小写)")
  }

  @Test
  def testFromText_CaseInsensitive(): Unit = {
    // 假设 101=image/jpeg 在 properties 中
    val mimeType = MimeTypeFactory.fromText("IMAGE/JPEG")
    assertEquals(273L, mimeType.code, "应不区分大小写查找")
    assertEquals("image/jpeg", mimeType.text, "Text 应被规范化为小写")
  }

  @Test
  def testFromText_Unknown(): Unit = {
    val invalidText = "application/x-non-existent"

    assertThrows(classOf[UnknownMimeTypeException], () => {
      MimeTypeFactory.fromText(invalidText)
      ()
    }, s"未知的 mime-type 文本 ($invalidText) 应抛出 UnknownMimeTypeException")
  }

  @Test
  def testGuessMimeType_Bytes(): Unit = {
    // 测试 PNG
    val pngType = MimeTypeFactory.guessMimeType(PNG_MAGIC)
    assertEquals(278L, pngType.code, "PNG 字节应被识别为 code 100")
    assertEquals("image/png", pngType.text, "PNG 字节应被识别为 'image/png'")

    // 测试 PDF
    val pdfType = MimeTypeFactory.guessMimeType(PDF_MAGIC)
    assertEquals(46L, pdfType.code, "PDF 字节应被识别为 code 200")
    assertEquals("application/pdf", pdfType.text, "PDF 字节应被识别为 'application/pdf'")

    // 测试 Text
    val textType = MimeTypeFactory.guessMimeType(TEXT_DATA)
    assertEquals(44L, textType.code, "Text 字节应被识别为 code 300")
    assertEquals("application/octet-stream", textType.text, "Text 字节应被识别为 'application/octet-stream'")
  }

  @Test
  def testGuessMimeType_EmptyBytes(): Unit = {
    // 假设 -1=unknown/unknown 在 properties 中
    val unknownType = MimeTypeFactory.guessMimeType(EMPTY_DATA)
    assertEquals(44L, unknownType.code, "空字节数组应返回默认 code -1")
    assertEquals("application/octet-stream", unknownType.text, "空字节数组应返回 'application/octet-stream'")
  }

  @Test
  def testGuessMimeType_InputStream(): Unit = {
    // (该方法会读取整个流)
    val is = new ByteArrayInputStream(PNG_MAGIC)
    val pngType = MimeTypeFactory.guessMimeType(is)
    is.close()

    assertEquals(278L, pngType.code, "PNG 输入流应被识别为 code 100")
    assertEquals("image/png", pngType.text, "PNG 输入流应被识别为 'image/png'")
  }

  @Test
  def testGuessMimeTypeWithPrefix_ShortStream(): Unit = {
    // (该方法只读取前缀)
    val is = new ByteArrayInputStream(PDF_MAGIC)
    val pdfType = MimeTypeFactory.guessMimeTypeWithPrefix(is)
    is.close()

    assertEquals(46L, pdfType.code, "PDF 输入流 (短) 应被识别为 code 200")
    assertEquals("application/pdf", pdfType.text, "PDF 输入流 (短) 应被识别为 'application/pdf'")
  }

  @Test
  def testGuessMimeTypeWithPrefix_LongStream(): Unit = {
    // (该方法只读取前缀 4KB)
    // 创建一个大于 4KB 的流，但 magic number 在开头
    val junk = new Array[Byte](5000)
    val longData = PDF_MAGIC ++ junk
    val is = new ByteArrayInputStream(longData)

    val pdfType = MimeTypeFactory.guessMimeTypeWithPrefix(is)
    is.close()

    assertEquals(46L, pdfType.code, "长输入流的 PDF 头应被正确识别")
    assertEquals("application/pdf", pdfType.text, "长输入流的 PDF 头应被正确识别")
  }

  @Test
  def testGuessMimeTypeWithPrefix_EmptyStream(): Unit = {
    // 假设 -1=unknown/unknown 在 properties 中
    val is = new ByteArrayInputStream(EMPTY_DATA)
    val unknownType = MimeTypeFactory.guessMimeTypeWithPrefix(is)
    is.close()

    assertEquals(44L, unknownType.code, "空输入流应返回默认 code -1")
    assertEquals("application/octet-stream", unknownType.text, "空输入流应返回 'application/octet-stream'")
  }
}
