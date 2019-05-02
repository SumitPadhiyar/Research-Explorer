package com.iitm.research_explorer

import org.apache.spark.sql.SparkSession

/*
 * Command Line input: Path to json
 * */
object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("research-explorer")
    /*
    .config("spark.master", "local")
    .config("spark.executor.cores", "8")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    */
    .getOrCreate()

  spark.sparkContext.setCheckpointDir("/tmp")

  // Change log level - use INFO for more descriptive logs
  spark.sparkContext.setLogLevel("WARN")

  val df = spark.read.json("s3://com.iitm.researchexplorer.dataset/s2-large")
  //val df = spark.read.json(args(0))

  val citationGraph = new CitationGraph(df)

  val collaborationGraph = new CollaborationGraph(df, spark)

  val publicationGraph = new PublicationGraph(df, spark)
  println("Number of vertices: " + publicationGraph.graph.vertices.count)
  println("Number of edges: " + publicationGraph.graph.edges.count)

  val paperRank = new PaperRank(publicationGraph, spark)
  paperRank.displayPaperRankings()
  paperRank.displayAuthorRankings()
  paperRank.displayVenueRankings()

  val queries = new Queries(publicationGraph, spark)
  queries.mostCitedPapers()
  queries.mostPopularAuthors()
  queries.mostPopularVenues()
  queries.mostCitedAuthors()
  spark.stop()

}
