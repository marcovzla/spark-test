name := "recommender"

version := "1.0"

organization := "org.clulab"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
)
