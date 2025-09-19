package link.rdcn.util

import org.apache.commons.io.IOUtils
import eu.medsea.mimeutil.MimeUtil

import java.io.InputStream
import java.util.Properties
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `properties AsScalaMap`}
/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/19 19:47
 * @Modified By:
 */
case class MimeType(code: Long, text: String) {
  def major: String = text.split("/")(0);

  def minor: String = text.split("/")(1);
}

object MimeTypeFactory {
  MimeUtil.registerMimeDetector("eu.medsea.mimeutil.detector.MagicMimeMimeDetector");

  private val properties = new Properties();
  properties.load(this.getClass.getClassLoader.getResourceAsStream("mime.properties"));

  private val code2Types = properties.map{ case (str, str1) => str.toLong -> str1.toLowerCase}.toMap

  private val type2Codes = code2Types.map(x => (x._2, x._1))

  def fromText(text: String): MimeType =
    MimeType(type2Codes.getOrElse(text.toLowerCase, throw new UnknownMimeTypeException(text)), text.toLowerCase)

  private def fromCode(code: Long) = MimeType(code, code2Types(code));

  def guessMimeType(inputStream: InputStream): MimeType =
    MimeUtil.getMimeTypes(IOUtils.toByteArray(inputStream))
      .headOption.map(mt => fromText(mt.toString)).getOrElse(fromCode(-1))

  def guessMimeType(bytes: Array[Byte]): MimeType = MimeUtil.getMimeTypes(bytes)
    .headOption.map(mt => fromText(mt.toString)).getOrElse(fromCode(-1))
}

class UnknownMimeTypeException(typeName: String) extends RuntimeException(s"Unknown mime-type: $typeName.")
