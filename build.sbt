name := "twitter-mining"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.1",
  "org.apache.spark" %% "spark-sql" % "1.5.1",
  "org.apache.spark" %% "spark-graphx" % "1.5.1")

    