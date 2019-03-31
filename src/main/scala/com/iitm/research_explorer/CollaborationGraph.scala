package com.iitm.research_explorer

import org.apache.spark.sql.{SparkSession, functions, DataFrame}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

class CollaborationGraph (val inpDf: DataFrame, spark: SparkSession) {

  import spark.implicits._

  val df = inpDf.as[PaperMetaData].select("authors").as[Array[Author]]


  val authorDF = df.withColumn("authors", explode($"authors"))
    .select($"authors.ids", $"authors.name".as("name"))
    .as[Author]
    .filter(author => author.ids.length == 1 && !author.name.isEmpty)
    .select($"name", explode($"ids").as("id"))

  val removeEmpty = udf((array: Seq[Any]) => array.nonEmpty)

  val authorEdgeDF = df.select("authors").as[Array[Author]]
    .map(authors => for (a <- authors if a.ids.length == 1; b <- authors if b.ids.length == 1; if a.ids(0) != b.ids(0)) yield (a.ids(0), b.ids(0))).withColumnRenamed("value", "authors")
    .filter(functions.size($"authors") > 0)
    .withColumn("authors", explode($"authors"))
    .select($"authors._1".as("src"), $"authors._2".as("dst"))

  // Build GraphFrame
  val graph = GraphFrame(authorDF, authorEdgeDF)

  println(graph.degrees.show(10))

}
