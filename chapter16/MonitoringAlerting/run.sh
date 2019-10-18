#!/bin/bash
mvn clean package
java -jar ./target/monitoring-alerting-1.0-SNAPSHOT.jar
