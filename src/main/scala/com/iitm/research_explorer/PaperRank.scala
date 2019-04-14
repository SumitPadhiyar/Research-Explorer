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
* We keep track of rank of each vertex. Whenever there is a change in rank of the vertex,
* it propagates the change to its neighbor in the form of delta (current_rank - previous_rank).
* This delta can then simply be added by each vertex to its own rank.
* Initialize rank with the degree of each vertex
* To compute the delta, we keep track of both the current and previous rank.
* Initially previous_rank = 1 and current_rank = 1 / deg(v)

 */
class PaperRank (graph: GraphFrame) {

  val ranks = graph.pregel
    // Column previous_rank. After the messages are received, it stores the current
    // value of rank
    .withVertexColumn("prev_rank", lit(1.0), Pregel.src("rank"))
    // Column current rank. Normalizes the rank after adding all the messages
    .withVertexColumn("rank",  lit(lit(1.0) / Pregel.src("degree")),
      (coalesce(Pregel.msg, lit(0.0)) + Pregel.src("rank"))
        /Pregel.src("degree"))
    // TODO: Send messages to neighbors only when the current rank is different from previous rank
    .sendMsgToDst(Pregel.src("rank") - Pregel.src("prev_rank"))
    // Sum messages received from the neighbors
    .aggMsgs(sum(Pregel.msg))
    .run()
}
