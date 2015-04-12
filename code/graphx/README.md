To run the simulation :

spark-submit --class "com.bd.propogation.ic.Simulation" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar data/random.txt data/random/output-degree/40/ 100 0.01

To run Independent Cascade Greedy method:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar greedyic data/hep.txt 40 1 0.01 output-greedyic

To run Single Cycle method:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar singlecycle data/hep.txt 40 1 0.01 output-singlecycle

To run Edge Sampling Method

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar edgesampling data/hep.txt 40 100 0.01 output-edgesampling

To run Degree Method:

./spark/bin/spark-submit --class "com.bd.InfluenceMax" --master spark://ec2-54-81-22-17.compute-1.amazonaws.com:7077 spark-graphx-1.0.0-SNAPSHOT.jar degree /data/livejournal 50000 10 0.01 /data/output-degree

To run Random Method:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar random data/hep.txt 40 10 0.01 output-random

To run cc(connected components) methods:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar cc data/hep.txt 40 10 0.01 output-cc

To run Degree Discount methods:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar degreediscount data/hep.txt 50000 10 0.01 output-dd

To run Pagerank methods:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar pagerank data/hep.txt 40 10 0.01 output-pg

Run Driver :
spark-submit --class "com.bd.Driver" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar data/sparse.txt r40 10 0.01 data/sparse/output degree,random,edgesampling,pagerank

Run SimDriver :
spark-submit --class "com.bd.SimDriver" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar data/hep.txt 10 0.01

GraphUtil profiling :
spark-submit --class "com.bd.util.GraphUtilProfiling" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar data/phy.txt 1
