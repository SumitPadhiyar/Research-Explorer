name := "research-explorer"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

val sparkVersion = "2.4.0"

val graphFrameVersion = "0.7.0-spark2.4-s_2.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "graphframes" % "graphframes" % graphFrameVersion
)
