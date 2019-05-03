package com.iitm.research_explorer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.lib.Pregel

/**
  *
  * Rank authors, paper and venues.
  * Based on "A Graph Analytics Framework for Ranking Authors, Papers and Venues"
  * [https://arxiv.org/pdf/1708.00329.pdf]
  *
  * Uses GraphFrame's Pregel API:
  * https://graphframes.github.io/graphframes/docs/_site/api/scala/index.html#org.graphframes.lib.Pregel
  * Implementation ideas:
  * We keep track of rank of each vertex. Whenever there is a change in rank of the vertex,
  * it propagates the change to its neighbor in the form of delta (current_rank - previous_rank).
  * This delta can then simply be added by each vertex to its own rank.
  * Initialize rank with the degree of each vertex
  * To compute the delta, we keep track of both the current and previous rank.
  * Initially previous_rank = 1 and current_rank = 1 / deg(v)
  */

class PaperRank(publicationGraph: PublicationGraph, sparkSession: SparkSession) {

  private var rankDF: Option[DataFrame] = None
  val ROWS = 50 // Shows top 50 rows in all the queries


  def displayAuthorRankings(): Unit = {

    if (rankDF.isEmpty) {
      execute()
    }

    val df = rankDF.get

    println("Author rank:")
    df.where(col("type").equalTo(VertexType.Author.toString))
      .drop(df("type")).drop(df("degree")).drop(df("prev_rank"))
      .show(ROWS, false)
  }

  def displayPaperRankings(): Unit = {

    if (rankDF.isEmpty) {
      execute()
    }

    val df = rankDF.get

    println("Paper rank:")
    df.where(col("type").equalTo(VertexType.Paper.toString))
      .drop(df("type")).drop(df("degree")).drop(df("prev_rank"))
      .show(ROWS)
  }

  def execute(): Option[DataFrame] = {

    if (rankDF.isDefined) {
      return rankDF
    }

    val DF = publicationGraph.graph.pregel
      // Column current rank. Normalizes the rank after adding all the messages
      .withVertexColumn("rank", lit(1.0), coalesce(Pregel.msg, lit(0.0)) + col("rank"))
      // Column previous_rank. After the messages are received, it stores the current
      // value of rank
      .withVertexColumn("prev_rank", lit(0.0), col("rank"))
      // TODO: Send messages to neighbors only when the current rank is different from previous rank
      .sendMsgToDst((Pregel.src("rank") - Pregel.src("prev_rank")) / Pregel.src("degree"))
      // Sum messages received from the neighbors
      .aggMsgs(sum(Pregel.msg))
      .setMaxIter(1)
      .run()
      .sort(desc("rank"))

    rankDF = Option(DF)
    rankDF
  }

  def displayVenueRankings(): Unit = {

    if (rankDF.isEmpty) {
      execute()
    }

    val df = rankDF.get

    println("Venue rank:")
    df.where(col("type").equalTo(VertexType.Venue.toString))
      .drop(df("type")).drop(df("degree")).drop(df("prev_rank")).drop(df("name"))
      .show(ROWS, false)
  }


}
