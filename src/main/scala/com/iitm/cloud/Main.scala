package com.iitm.cloud

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("research-explorer")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setCheckpointDir("/tmp")

  // Read data from json
  val df = spark.read.json("/home/diptanshu/Downloads/Datasets/semantic-scholar/sample-S2-records.json")

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
  //println(graph.degrees.show(10))
  //val results = graph.pageRank.resetProbability(0.15).tol(0.001).run()
  //results.vertices.show(100)
  val result = graph.connectedComponents.run()

  vertexDF.count()
  edgeDF.count()
  result.agg(countDistinct("component"))
  result.show(10)

  //val results = graph.triangleCount.run()
  //results.sort(desc("count")).select("id", "count").show()
  //val result = graph.labelPropagation.maxIter(5).run()
  //result.orderBy("label").show(20)


  // Create graphs for PaperRank
  // Paper vertices
  df.select("id").withColumn("TYPE", lit("paper"))

  // Author vertices
  df.select("authors").withColumn("authors", explode(df("authors")))
    .select($"authors".getField("ids").getItem(0)).withColumn("TYPE", lit("author"))
    .withColumnRenamed("authors.ids[0]", "id")
  
  // val a = df.select("authors").filter((size($"authors") !== 0) && (size($"authors".getItem(0).getField("ids")) !== 0)).select($"authors".getItem(0).getField("ids").getItem(0));

  // Journal vertices
  df.select("journalName").withColumn("TYPE", lit("venue"))
    .withColumnRenamed("journalName", "id")


  // Author-Author edges
  df.select("authors")
    .filter((size($"authors") =!= 0) && (size($"authors".getItem(0).getField("ids")) =!= 0))
    .withColumn("coauthors", explode(df("authors")))

  // Paper-Paper edges
  df.select("id", "outCitations")
    .withColumn("outCitations", explode(df("outCitations")))
    .withColumnRenamed("id", "src")
    .withColumnRenamed("outCitations","dst")

  // Paper-Author edges
  // Author-Venue edges
  // Paper-Venue edges

  // Stop spark session
  spark.stop()

}
