package com.iitm.research_explorer

import org.apache.spark.sql.functions._

/**
  * Query the underlying graphs to derive insights about
  * the data.
  */
class Queries (publicationGraph: PublicationGraph) {

  val ROWS = 50     // Shows top 50 rows in all the queries

  /*
  * Finds the top most cited authors.
  * */
  def mostCitedAuthors(): Unit = {

  }

  /*
  * Finds the top venues which have most papers submissions.
  * */
  def mostPopularVenues(): Unit = {
    publicationGraph.paperVenueEdgesDF
      .groupBy("dst")
      .agg(count("dst"))
      .sort(desc("count(dst)"))
      .show(ROWS)
  }

  /*
  * Finds the top most cited papers.
  * */
  def mostCitedPapers(): Unit = {
    publicationGraph.paperPaperEdgesDF
      .groupBy("dst")
      .agg(count("dst"))
      .sort(desc("count(dst)"))
      .show(ROWS)
  }

   /*
  * Finds the top authors which have most number of papers.
  * */
  def mostPopularAuthors(): Unit = {
    publicationGraph.paperAuthorEdgesDF
      .groupBy("dst")
      .agg(count("dst"))
      .sort(desc("count(dst)"))
      .show(ROWS)
  }

}
