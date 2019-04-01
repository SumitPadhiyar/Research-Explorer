package com.iitm.research_explorer

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class PublicationGraph(val df: DataFrame, sparkSession: SparkSession) {

  import sparkSession.implicits._

  private var paperVenueEdgesDF: DataFrame = _

  def generatePaperVenueEdgesDF(): Unit = {
    paperVenueEdgesDF = df.select($"id".as("src"), $"venue".as("dst"), functions.lit(EdgeType.PaperToVenue.toString).as("type"))
      .filter(row => !row.getAs[String](0).isEmpty && !row.getAs[String](1).isEmpty)
    paperVenueEdgesDF.printSchema()
    paperVenueEdgesDF.show()
  }
}
