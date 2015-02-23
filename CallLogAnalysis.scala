import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

var pathGraph = GraphLoader.edgeListFile(sc, "data/spark_edges10000")

var ranks = pathGraph.pageRank(0.0001).vertices

var services = (sc.textFile("data/spark_vertex"))
	.map(line => line.split(",")).map(fields => (fields(0).toLong, fields(1)))

val ranksByDesc = services.join(ranks).map {
	case (id, (desc, rank)) => (desc, rank)
}

println(ranksByDesc.top(5)(Ordering.by(_._2)).mkString("\n"))