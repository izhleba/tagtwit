import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

object TwitterAuth {

  def auth(authFile: String): Unit = {
    val path = Paths.get(authFile)
    val reader = Files.newBufferedReader(path)
    val authPrefix = "twitter4j.oauth."
    try {
      reader.lines().iterator().asScala.zipWithIndex.foreach { case (line,index) =>
        val values = line.split(";")
        if (values.size != 2 || !values(0).startsWith(authPrefix)) {
          sys.error(s"Wrong file format line:$index.${authFile}")
        } else {
          System.setProperty(values(0), values(1))
        }
      }
    } finally {
      reader.close()
    }
  }
}
