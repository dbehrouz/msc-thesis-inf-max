#!/bin/bash

spark-submit --class "com.bd.propogation.ic.Simulation" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar data/hep.txt data/active.txt 1 0.2