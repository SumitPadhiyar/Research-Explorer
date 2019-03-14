package com.iitm.cloud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.graphframes.GraphFrame

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("research-explorer")
    .config("spark.master", "local")
    .getOrCreate()

  // Read data from json
  val df = spark.read.json("/home/diptanshu/Downloads/Datasets/semantic-scholar/sample-S2-records")

  // Vertex DataFrame
  val vertexDF = df.select("id")

  // Edge DataFrame
  // NOTE: GraphFrame requires edge DataFrame to have column named as "src" and "dst"
  val edgeDF = df.select("id", "outCitations")
                  .withColumn("outCitations", explode(df("outCitations")))
                  .withColumnRenamed("id", "src")
                  .withColumnRenamed("outCitations","dst")

  // Build GraphFrame
  val graph = GraphFrame(vertexDF, edgeDF)

  // Print degree of 10 vertices
  println(graph.degrees.show(10))

  // Stop spark session
  spark.stop()

}
