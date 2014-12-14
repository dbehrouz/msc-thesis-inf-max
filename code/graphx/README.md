To run the simulation :

spark-submit --class "com.bd.propogation.ic.Simulation" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar data/hep.txt output/ 10000 0.05

To run Independent Cascade Greedy method:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar greedyic data/hep.txt 40 1 0.01 output-greedyic

To run Edge Sampling Method

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar edgesampling data/hep.txt 40 10 0.01 output-edgesampling

To run Degree Method:

spark-submit --class "com.bd.InfluenceMax" --master local[*] target/spark-graphx-1.0.0-SNAPSHOT.jar degree data/hep.txt 40 10 0.01 output-degree
