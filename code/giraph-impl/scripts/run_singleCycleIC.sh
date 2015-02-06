#!/bin/sh
hadoop dfs -copyFromLocal /ic/input/col/hep
hadoop jar target/giraph-hadoop-1.0-SNAPSHOT.jar org.apache.giraph.GiraphRunner com.bd.propagation.function.ic.singlecycle.SimpleSingleCycle -vif com.bd.formats.JsonLongComplexFloatInputFormat -vip /ic/input/col/hep -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /ic/output/hep -w 1


hadoop jar target/giraph-hadoop-1.0-SNAPSHOT.jar com.bd.mapreduce.TopN /ic/output/hep /ic/output/final 40
