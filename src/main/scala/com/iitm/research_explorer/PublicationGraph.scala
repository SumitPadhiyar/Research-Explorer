package com.iitm.research_explorer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.DST
import org.graphframes.GraphFrame.SRC

class PublicationGraph(val df: DataFrame, sparkSession: SparkSession) {

  import sparkSession.implicits._

  private var paperVenueEdgesDF: DataFrame = _
  private var paperAuthorEdgesDF: DataFrame = _
  private var authorVenueEdgesDF: DataFrame = _
  private var authorAuthorEdgesDF: DataFrame = _
  private var paperPaperEdgesDF: DataFrame = _

  private var authorVerticesDF: DataFrame = _
  private var paperVerticesDF: DataFrame = _
  private var venueVerticesDF: DataFrame = _

  var graph: GraphFrame = _

  // Initialize graph
  generateGraph()

  def generatePaperVenueEdgesDF(): Unit = {
    paperVenueEdgesDF = df.select($"id".as("src"), $"venue".as("dst"))
      .filter(row => !row.getAs[String](0).isEmpty && !row.getAs[String](1).isEmpty)

    paperVenueEdgesDF = symmetrize(paperVenueEdgesDF)
      .withColumn("type", lit(EdgeType.PaperToVenue.toString))

    paperVenueEdgesDF.printSchema()
    paperVenueEdgesDF.show()
  }

  def generatePaperAuthorEdgesDF(): Unit = {
    paperAuthorEdgesDF = df.select($"id", $"authors")
      .withColumn("authors", explode($"authors"))
      .filter(size($"authors".getField("ids")) === 1)
      .withColumnRenamed("id", "src")
      .withColumn("dst", $"authors".getField("ids").getItem(0))
      .drop("authors")

    paperAuthorEdgesDF = symmetrize(paperAuthorEdgesDF)
      .withColumn("type", lit(EdgeType.PaperToAuthor.toString))

    paperAuthorEdgesDF.printSchema()
    paperAuthorEdgesDF.show()
  }

  def generateAuthorVenueEdgesDF(): Unit = {
    authorVenueEdgesDF = df.select($"venue", $"authors", lit(EdgeType.AuthorToVenue.toString).as("type"))
      .withColumn("authors", explode($"authors"))
      .filter(size($"authors".getField("ids")) === 1 && length($"venue") =!= 0)
      .withColumnRenamed("venue", "src")
      .withColumn("dst", $"authors".getField("ids").getItem(0))
      .drop("authors")

    authorVenueEdgesDF = symmetrize(authorVenueEdgesDF)
      .withColumn("type", lit(EdgeType.AuthorToVenue.toString))


    authorVenueEdgesDF.printSchema()
    authorVenueEdgesDF.show()
  }

  def generateAuthorAuthorEdgesDF(): Unit = {
    authorAuthorEdgesDF = df.select("authors").as[Array[Author]]
      .map(authors => for (a <- authors if a.ids.length == 1; b <- authors if b.ids.length == 1; if a.ids(0) != b.ids(0)) yield (a.ids(0), b.ids(0))).withColumnRenamed("value", "authors")
      .filter(size($"authors") > 0)
      .withColumn("authors", explode($"authors"))
      .select($"authors._1".as("src"), $"authors._2".as("dst"))

    authorAuthorEdgesDF = symmetrize(authorAuthorEdgesDF)
      .withColumn("type", lit(EdgeType.AuthorToAuthor.toString))

    authorAuthorEdgesDF.printSchema()
    authorAuthorEdgesDF.show()
  }

  // NOTE: Paper to paper edges are directed
  def generatePaperPaperEdgesDF(): Unit = {
    paperPaperEdgesDF = df.select("id", "outCitations")
      .withColumn("outCitations", explode(df("outCitations")))
      .withColumnRenamed("id", "src")
      .withColumnRenamed("outCitations","dst")
      .withColumn("type", lit(EdgeType.PaperToPaper.toString))

    paperPaperEdgesDF.printSchema()
    paperPaperEdgesDF.show()
  }

  def generateVertices(): Unit = {
    authorVerticesDF = df.withColumn("authors", explode($"authors"))
      .select($"authors.ids")
      .as[Array[String]]
      .filter(authorids => authorids.length == 1)
      .select(explode($"ids").as("id"))
      .withColumn("type", lit(VertexType.Author.toString))

    paperVerticesDF = df.select("id")
                        .withColumn("type", lit(VertexType.Paper.toString))

    venueVerticesDF = df.select($"venue".as("id")).filter(length($"id") =!= 0)
                        .withColumn("type", lit(VertexType.Venue.toString))

    authorVerticesDF.printSchema()
    authorVerticesDF.show()
    paperVerticesDF.printSchema()
    paperVerticesDF.show()
    venueVerticesDF.printSchema()
    venueVerticesDF.show()
  }

  /**
    * Returns the symmetric directed graph of the graph specified by input edges.
    * @param ee non-bidirectional edges
    */
  def symmetrize(ee: DataFrame): DataFrame = {
    val EDGE = "_edge"
    ee.select(explode(array(
      struct(col(SRC), col(DST)),
      struct(col(DST).as(SRC), col(SRC).as(DST)))
    ).as(EDGE))
      .select(col(s"$EDGE.$SRC").as(SRC),
              col(s"$EDGE.$DST").as(DST))
  }


  def generateGraph(): Unit = {
    // Create vertices and edges dataframes
    generateVertices()
    generateAuthorAuthorEdgesDF()
    generateAuthorVenueEdgesDF()
    generatePaperAuthorEdgesDF()
    generatePaperPaperEdgesDF()
    generatePaperVenueEdgesDF()

    // Union all the vertices and edges into two dataframes
    val vertexDF = authorVerticesDF.union(paperVerticesDF).union(venueVerticesDF)
    val edgeDF = authorAuthorEdgesDF.union(authorVenueEdgesDF)
                  .union(paperAuthorEdgesDF)
                  .union(paperPaperEdgesDF)
                  .union(paperVenueEdgesDF)

    // Create GraphFrame
    graph = GraphFrame(vertexDF, edgeDF)

  }
}
