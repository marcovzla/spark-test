# recommender

## Instructions to run recommender on AWS EMR.

https://aws.amazon.com/emr/


### package scala project into a jar

Leave the SparkContext unconfigured. We want to use EMR's defaults.

Use sbt-assembly. First, add it to the file `project/plugins.sbt`:

    addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")

Remember to specify that spark is provided. In `build.sbt`:

    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"

Run the command `sbt assembly`. The fat jar should be in `target/scala-2.11`.

### upload data

Upload data and fat jar to s3 bucket so that they can be retrieved from the cluster.

### start emr cluster

- set cluster name
- no logging
- select spark 2.2
- set ec2 key pair
- create cluster (takes about 5 minutes; should say "waiting")

### prepare to run
- remember to change security to accept ssh connections from master
- ssh to master node
- download fat jar and names file

    aws s3 cp s3://lum-ai-bucket/ml-1m/recommender-assembly-1.0.jar .
    aws s3 cp s3://lum-ai-bucket/ml-1m/movies.dat .

https://stackoverflow.com/a/42091255

### run

    spark-submit recommender-assembly-1.0.jar
