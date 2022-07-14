package com.jd.easy.audience.task.dataintegration.util

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Base64

import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.jd.easy.audience.common.exception.JobInterruptedException
import com.jd.easy.audience.task.driven.segment.Segment
import org.slf4j.LoggerFactory
import org.reflections.Reflections
import scala.collection.JavaConversions
import collection.JavaConverters._
import scala.collection.immutable.Map

/**
 * 序列化的工具类
 */
class CustomSerializeUtils {
  private val BASE_PARENT_CLASS_PATH = "com.jd.easy.audience.task.driven"
  private val INTEGRATION_CLASS_PATH = "com.jd.easy.audience.task.dataintegration"
  private val BEAN_CLASS_PATH = "com.jd.easy.audience.task.commonbean"

  private def getObjectMapper(): ObjectMapper = {

    val om: ObjectMapper  = new ObjectMapper

    println("class: " + whereFrom(om))
    val reflections = new Reflections(BASE_PARENT_CLASS_PATH)
    val beans: Reflections = new Reflections(BEAN_CLASS_PATH)
    val integration: Reflections = new Reflections(INTEGRATION_CLASS_PATH)
    reflections.merge(integration).merge(beans)
    val classSeta:java.util.Set[Class[_]] = reflections.getTypesAnnotatedWith(classOf[JsonTypeName])

    for (cls <- classSeta.asScala) {
      om.registerSubtypes(cls)
    }

    return om
  }

  def whereFrom(o: Any): String = {
    if (o == null) return null
    val c = o.getClass
    var loader = c.getClassLoader
    if (loader == null) { // Try the bootstrap classloader - obtained from the ultimate parent of the System Class Loader.
      loader = ClassLoader.getSystemClassLoader
      while ( {
        loader != null && loader.getParent != null
      }) loader = loader.getParent
    }
    if (loader != null) {
      val name = c.getCanonicalName
      val resource = loader.getResource(name.replace(".", "/") + ".class")
      if (resource != null) return resource.toString
    }
    "Unknown"
  }
}

object CustomSerializeUtils {

  private val om: ObjectMapper = new CustomSerializeUtils().getObjectMapper()
  private val LOGGER = LoggerFactory.getLogger(CustomSerializeUtils.getClass)

  /**
   * 对传入的对象转成json后进行base64加密，可以屏蔽掉一些特殊字符的干扰，方便传输
   *
   * @param objectValue
   * @return
   */
  def encodeAfterObject2Json(objectValue: Any): String = {
    val serializeStr = serialize(objectValue)
    new String(Base64.getEncoder.encode(serializeStr.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8)
  }

  /**
   * 反序列化成对象
   *
   * @param jsonContext
   * @return
   */
  def createSegmentObject(jsonContext: String): Segment = {
    try {
      val segment = deserialize(jsonContext, classOf[Segment])
      segment.validate()
      segment
    } catch {
      case e: Exception =>
        LOGGER.error("Failed to resolve def {}, error: {}", jsonContext, e.getMessage, e)
        throw new RuntimeException("Failed to resolve def" + e.getMessage, e)
    }
  }

  def deserialize[T](json: String, tClass: Class[T]): T = {
    try {
      om.readValue(json, tClass)
    }
    catch {
      case var3: IOException =>
        LOGGER.error(s"""json deserialize error. keys={$json}""", var3)
        throw new JobInterruptedException("反序列化对象失败", "Error: json deserialize error")
    }
  }

  def serialize(`object`: Any): String = {
    try om.writeValueAsString(`object`)
    catch {
      case e: JsonProcessingException =>
        LOGGER.error("serialize " + `object` + " error : " + e.getMessage, e)
        throw new RuntimeException(e.getMessage, e)
    }
  }
  def regJson(json:Option[Any]) = json match {
    case Some(map: Map[String, Any]) => map
  }

}
