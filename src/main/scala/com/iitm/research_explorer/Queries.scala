package com.iitm.research_explorer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame.{DST, SRC}

/**
  * Query the underlying graphs to derive insights about
  * the data.
  */
class Queries(publicationGraph: PublicationGraph, sparkSession: SparkSession) {

  val ROWS = 50 // Shows top 50 rows in all the queries

  /*
  * Finds the top most cited authors.
  * */
  def mostCitedAuthors(): Unit = {
    var paperAuthorEdgesDF = publicationGraph.paperAuthorEdgesDF
    var paperPaperEdgesDF = publicationGraph.paperPaperEdgesDF
    var authorVerticesDF = publicationGraph.authorVerticesDF

    paperPaperEdgesDF
      .join(paperAuthorEdgesDF, paperPaperEdgesDF.col(DST) === paperAuthorEdgesDF.col(SRC))
      .drop(paperPaperEdgesDF.col(SRC))
      .drop(paperPaperEdgesDF.col(DST))
      .drop(paperAuthorEdgesDF.col(SRC))
      .drop(paperPaperEdgesDF.col("type"))
      .drop(paperAuthorEdgesDF.col("type"))
      .groupBy("dst")
      .agg(count("dst"))
      .withColumnRenamed("count(dst)", "citations")
      .join(authorVerticesDF, col(DST) === authorVerticesDF.col("id"))
      .sort(desc("citations"))
      .drop(authorVerticesDF.col("type"))
      .drop(authorVerticesDF.col("id"))
      .show(ROWS)
  }

  /*
  * Finds the top venues which have most papers submissions.
  * */
  def mostPopularVenues(): Unit = {
    var venueVerticesDF = publicationGraph.venueVerticesDF

    publicationGraph.paperVenueEdgesDF
      .groupBy("dst")
      .agg(count("dst"))
      .withColumnRenamed("count(dst)", "citations")
      .drop(col("src"))
      .join(venueVerticesDF, col(DST) === venueVerticesDF.col("id"))
      .sort(desc("citations"))
      .drop(col("dst"))
      .drop(col("type"))
      .drop(col("name"))
      .withColumnRenamed("id", "name")
      .show(ROWS)
  }

  /*
  * Finds the top most cited papers.
  * */
  def mostCitedPapers(): Unit = {
    var paperVerticesDF = publicationGraph.paperVerticesDF
    publicationGraph.paperPaperEdgesDF
      .groupBy("dst")
      .agg(count("dst"))
      .withColumnRenamed("count(dst)", "citations")
      .join(paperVerticesDF, col(DST) === col("id"))
      .sort(desc("citations"))
      .drop(paperVerticesDF.col("type"))
      .drop(paperVerticesDF.col("id"))
      .drop(col("dst"))
      .show(ROWS)
  }

  /*
 * Finds the top authors which have most number of papers.
 * */
  def mostPopularAuthors(): Unit = {
    var authorVerticesDF = publicationGraph.authorVerticesDF

    publicationGraph.paperAuthorEdgesDF
      .groupBy("dst")
      .agg(count("dst"))
      .withColumnRenamed("count(dst)", "citations")
      .join(authorVerticesDF, col(DST) === authorVerticesDF.col("id"))
      .sort(desc("citations"))
      .drop(authorVerticesDF.col("type"))
      .drop(authorVerticesDF.col("id"))
      .drop(col("dst"))
      .show(ROWS)
  }

}
