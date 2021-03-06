name := "spark-elections"

version := "0.1"

scalaVersion := "2.12.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

// https://mvnrepository.com/artifact/com.oracle/ojdbc6
libraryDependencies += "com.oracle" % "ojdbc6" % "12.1.0.1-atlassian-hosted"
