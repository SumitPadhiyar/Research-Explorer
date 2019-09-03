## Research Explorer
Research explorer answers interesting queries about research papers. Interesting queries that can be answered are
- Which authors collaborate frequently?
- Prominent author in particular research domain.
- Ranking of papers for a research topic.
- Journal rankings.

Ranking for paper, authors and venues is an implementation of the ranking algorithm as described in [A Graph Analytics Framework for Ranking Authors, Papers and Venues](http://www.mlgworkshop.org/2016/paper/MLG2016_paper_8.pdf). The dataset for ranking was obtained from open research corpus from semanticsscholar. Ranking algorithm was implemented on GraphFrames of Spark.

### Requirements

Scala: 2.11.12

Spark: 2.4.0


### Build and Run

```shell
sbt
sbt:research-explorer> compile
sbt:research-explorer> run
```

To run with JVM memory:

```shell
env JAVA_OPTS="-Xms4g -Xmx8g" sbt "run path_to_file"
```

