# Apache Kafka/Spark Streaming POC

## Overview

Mock data pipeline which reads a stream of weather data, aggregates it slightly, then saves it to a database.

## Architecture

	IoT devices --> Kafka --> Spark --> Cassandra  
