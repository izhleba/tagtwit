/**
  * All project dependencies
  */
val appDependencies = {
  val deps = Seq.newBuilder[ModuleID]

//  deps += "org.apache.spark" %% "spark-core" % "2.1.0" %  "provided"
  deps += "org.apache.spark" %% "spark-core" % "2.1.0"
//  deps += ("org.apache.spark" % "spark-streaming_2.11" % "2.1.0" %  "provided").exclude("org.apache.spark", "spark-tags_2.11-2.1.0")
  deps += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
  deps += "org.twitter4j" % "twitter4j" % "4.0.6"
//  deps += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.3"
  deps += "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.0"


  deps.result()
}

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "",
      scalaVersion := "2.11.7",
      version := "0.1.0-SNAPSHOT"
    )),
    name := "TwitMl",
    libraryDependencies ++= appDependencies
  )

//assemblyExcludedJars in assembly := {
//  val cp = (fullClasspath in assembly).value
//  cp filter { el =>
//    (el.data.getName == "unused-1.0.0.jar") ||
//      (el.data.getName == "spark-tags_2.11-2.1.0.jar")
//  }
//}
