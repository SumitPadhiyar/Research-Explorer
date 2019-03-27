package com.iitm.cloud

import org.graphframes.GraphFrame
import org.graphframes.lib.Pregel
import org.apache.spark.sql.functions._

/*
  Graph Structure:
    Vertex
      - Type
      - Authors
      - Papers
      - Venues
      - 
 */
object PaperRank {

  val graph: GraphFrame
  val numPapers = 1000
  val numAuthors = 1000
  val numVenues = 1000

  val ranks = graph.pregel
    .withVertexColumn("rank", lit(1.0 / (numPapers + numAuthors + numVenues)),
      coalesce(Pregel.msg, lit(0.0)) + Pregel.src("rank"))
    .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
    .aggMsgs(sum(Pregel.msg))
    .run()
}
