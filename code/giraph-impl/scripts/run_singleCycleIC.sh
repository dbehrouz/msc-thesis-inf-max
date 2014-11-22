#!/bin/sh
hadoop jar target/com.behrouz-1.0-SNAPSHOT.jar org.apache.giraph.GiraphRunner com.bd.propagation.function.ic.singlecycle.SimpleSingleCycle -vif com.bd.formats.JsonLongComplexFloatInputFormat -vip /ic/input/col/hep -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /ic/output/hep -w 1
