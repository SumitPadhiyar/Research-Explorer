package com.iitm.research_explorer

import org.apache.spark.sql.functions.explode
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame

class CitationGraph (val df: DataFrame) {

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

}
