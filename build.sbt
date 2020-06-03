lazy val root = (project in file(".")).
  settings(
    name := "GermanHousing",
    organization := "com.jentest",
    version := "1.0",
    scalaVersion := "2.11.12",
    test in assembly := {},
    mainClass in assembly := Some("Main.Main")
  )


val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.github.scopt" %% "scopt" % "3.6.0",
  // logging
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
)

target in assembly := file("target/")
assemblyJarName in assembly := "deutscheWohnung.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}