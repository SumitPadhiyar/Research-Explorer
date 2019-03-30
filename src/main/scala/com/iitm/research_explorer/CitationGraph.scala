package com.iitm.research_explorer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.graphframes.GraphFrame

class CitationGraph{

  def generate(): Unit ={

    val spark = SparkSession
      .builder()
      .appName("research-explorer")
      .config("spark.master", "local")
      .getOrCreate()

    // Read data from json
    val df = spark.read.json("/home/skip/MS/Jan_May_2019/Cloud_computing/research-explorer/dataset/sample-S2-records")

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


}
