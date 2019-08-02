package example.connector

import java.time.Instant

object JavaMapUtil {

  implicit class JavaMapExt(val javaMap: java.util.Map[String, AnyRef]) extends AnyVal {

    def getInt(key: String, default: => Int): Int = {
      val value = javaMap.get(key)
      if (value == null) default else value.toString.toInt
    }

    def getInstant(key: String, default: => Instant): Instant = {
      val value = javaMap.get(key)
      if (value == null) default else Instant.parse(value.toString)
    }
  }

}
