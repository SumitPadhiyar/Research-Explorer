## Research Explorer

Requires:

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

