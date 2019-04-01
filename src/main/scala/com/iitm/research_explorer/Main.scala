package com.iitm.research_explorer

import org.apache.spark.sql.SparkSession

/*
 * Command Line input: Path to json
 * */
object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("research-explorer")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setCheckpointDir("/tmp")

  val df = spark.read.json(args(0))

  val citationGraph = new CitationGraph(df)

  val collaborationGraph = new CollaborationGraph(df, spark)

  val publicationGraph = new PublicationGraph(df, spark)
  publicationGraph.generatePaperVenueEdgesDF()

  spark.stop()

}
