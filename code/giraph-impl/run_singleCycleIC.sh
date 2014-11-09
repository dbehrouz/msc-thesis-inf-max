#!/bin/sh
hadoop jar target/com.behrouz-1.0-SNAPSHOT.jar org.apache.giraph.GiraphRunner com.bd.propagation.ic.singlecycle.AllPropagationIC -vif com.bd.formats.JsonLongLongFloatLongVertexInputFormat -vip /user/behrouz/graph -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/behrouz/output/allic -w 1
