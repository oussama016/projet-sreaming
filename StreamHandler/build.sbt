name := "Stream Handler"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
	"org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
	"com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0",
	"com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0",
	"org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1" % "provided"
	
)
