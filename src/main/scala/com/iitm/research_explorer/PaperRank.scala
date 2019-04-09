package com.iitm.research_explorer

import org.graphframes.GraphFrame
import org.graphframes.lib.Pregel
import org.apache.spark.sql.functions._

/*
*
* Rank authors, paper and venues.
* Based on "A Graph Analytics Framework for Ranking Authors, Papers and Venues"
* [https://arxiv.org/pdf/1708.00329.pdf]
*
* Uses GraphFrame's Pregel API:
* https://graphframes.github.io/graphframes/docs/_site/api/scala/index.html#org.graphframes.lib.Pregel
* Implementation ideas:
* Initialize rank with the degree of each vertex
* Send change in rank (newRank - prevRank) as update to the connected edges.

 */
class PaperRank (graph: GraphFrame) {

  val ranks = graph.pregel
    .withVertexColumn("rank",  lit(1.0 / (numPapers + numAuthors + numVenues)),
      coalesce(Pregel.msg, lit(0.0)) + Pregel.src("rank"))
    .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
    .aggMsgs(sum(Pregel.msg))
    .run()
}
