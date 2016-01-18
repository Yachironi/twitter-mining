import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachironi on 15/12/15.
  */
object Preprocessing {

  def main(args: Array[String]) {

    val input = "/home/yachironi/git/twitter-mining/input/NewYorkOneWeek/NewYork-2015-2-23"
    val output = "/home/yachironi/git/twitter-mining/output"

    val conf = new SparkConf().setAppName("WorldCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json(input)
    val dataSetFiltred = df.select("entities.hashtags.text").rdd.map(row => row.getList(0)).filter(r => r.size() > 0).distinct()

    /**
      * Creating the co-occurrence graph
      * Map and reducing the nodes in order to see the co-occurence of
      * a Hashtag with another one in the same tweet
      *
      * The map and reduce are on top of Spark RDD, they are distributed
      */
    val coOccurenceMap = dataSetFiltred.flatMap(
      list => {
        list.toArray().map(e => e.asInstanceOf[String].toLowerCase).flatMap(
          hashtag1 => {
            list.toArray().map(e => e.asInstanceOf[String].toLowerCase).map(hashtag2 => {
              if (!hashtag1.equals(hashtag2)) {
                ((hashtag1, hashtag2), 1L)
                ((hashtag2, hashtag1), 1L)
              }
            })
          }
        )
      }
    ).filter(e => e.isInstanceOf[Tuple2[(String, String), Long]])
      .map(e => e.asInstanceOf[Tuple2[(String, String), Long]])
      .reduceByKey((valEdge1, valEdge2) => valEdge1 + valEdge2)


    // Id counter generator
    var globalCount = 1L

    /**
      * Extracting the vertexes from the coOccurenceMap
      * ((vertexIdSrc,vertexIdDest),coOccurrences) => Vertex(hashTag,VertexID)
      */
    val verticesMap: Map[String, VertexId] = coOccurenceMap.keys
      .flatMap(tuple => Array[String](tuple._1, tuple._2))
      .collect()
      .distinct // we have hashTag1->hashTag2 and hashTag2 -> hashTag1 , here we eliminate the duplicates
      .map(hashtag => {
      globalCount += 1
      (hashtag, globalCount)
    }
    ).toMap

    // Convert the verticesMap to Array of Tuple[(Long, String)]
    val vertexArray = verticesMap.map(vertex => (vertex._2, vertex._1)).toArray[(Long, String)]

    /**
      * Extracting the edges from the coOccurenceMap
      * ((vertexIdSrc,vertexIdDest),coOccurrences) => Edge[Long]
      * Edge class contain (srcVertexes , destVertexes)
      */
    val edgeArray = coOccurenceMap
      .map(edge => Edge(verticesMap.get(edge._1._1).get, verticesMap.get(edge._1._2).get, edge._2))
      .collect()


    /**
      * Constructing the graph, using GraphX library from Spark
      *
      */

    // Parallelize the the verteces and edges, using the Spark FrameWork
    val vertices: RDD[(VertexId, String)] = sc.parallelize(vertexArray)
    val edges: RDD[Edge[Long]] = sc.parallelize(edgeArray)
    val graph = Graph(vertices, edges)


    // Define a class to more clearly model the HashTag property
    case class HashTag(hashTag: String, inDeg: Int, outDeg: Int)
    // Create a hash tag Graph
    val initialHashTagGraph: Graph[HashTag, Long] = graph.mapVertices { case (id, hashTag) => HashTag(hashTag, 0, 0) }

    // Fill in the degree information
    val hashTagGraph = initialHashTagGraph.outerJoinVertices(initialHashTagGraph.inDegrees) {
      case (id, h, inDegOpt) => HashTag(h.hashTag, inDegOpt.getOrElse(0), h.outDeg)
    }.outerJoinVertices(initialHashTagGraph.outDegrees) {
      case (id, h, outDegOpt) => HashTag(h.hashTag, h.inDeg, outDegOpt.getOrElse(0))
    }


    /*
       for ((id, property) <- hashTagGraph.vertices.collect) {
          println(s"Hash '${property.hashtag}' co-occurred with ${property.inDeg + property.outDeg} hashtags.")
        }*/

    var vpred = null
    // hashTagGraph.subgraph(vpred = (id, hashtag) => hashtag.inDeg + hashtag.outDeg >= 100).vertices.foreach(println)

    //val prGraph = hashTagGraph.staticPageRank(5).cache

    val ccGraph = hashTagGraph.connectedComponents()


    /*
    hashTagGraph.triplets.foreach {
      edgeTriplet => println(s"${edgeTriplet.srcAttr.hashtag} ==${edgeTriplet.attr}==> ${edgeTriplet.dstAttr.hashtag}}")
    }*/

    hashTagGraph.triplets.foreach(edge => {
      printf(edge.srcAttr.hashTag + "--" + edge.attr + "-->" + edge.srcAttr.hashTag)
    })

    //hashTagGraph.triplets.filter(triple => Seq("", "").contains(triple.dstAttr.hashtag))
    //hashTagGraph.convertToCanonicalEdges().triplets
    /*
        val stopWords = Seq("newyork", "nyc", "ny", "newyorkcity")


        hashTagGraph.convertToCanonicalEdges().triplets.filter(triple => (!stopWords.contains(triple.dstAttr.hashtag) && !stopWords.contains(triple.srcAttr.hashtag))).top(500) {
          Ordering.by((entry: EdgeTriplet[Hashtag, Long]) => entry.attr)
        }.foreach {
          edgeTriplet => println(s"${edgeTriplet.srcAttr.hashtag} ==${edgeTriplet.attr}==> ${edgeTriplet.dstAttr.hashtag}")
        }*/
/*
    val joined = hashTagGraph.outerJoinVertices(ccGraph.vertices) {
      (vid, vd, cc) => (vd, cc)
    }.vertices.groupBy(tuple => tuple._2._2)
      .foreach(coGroup => println(coGroup._1.get + "=>" + coGroup._2.map(x => x._2._1.hashTag)))
*/

    /*

        hashTagGraph.vertices.leftJoin(ccGraph.vertices) {
          case (vertexId, vertex, component) => s"${vertex.hashtag} is in component ${component.get}"
        }.groupByKey().foreach(println)*/

    /*
    hashTagGraph.vertices.leftJoin(ccGraph.vertices) {
      case (vertexId, vertex, component) => s"${vertex.hashtag} is in component ${component.get}"
    }.foreach { case (id, str) => println(str) }
    */
    /*
        hashTagGraph.connectedComponents().outerJoinVertices(hashTagGraph.vertices) {
          (vertexId, value, vertex) => (value, vertex.get.hashtag)
        }.vertices.foreach(println)*/
    /*
        prGraph.outerJoinVertices(hashTagGraph.vertices) {
          (vertexId, value, vertex) => (value, vertex.get.hashtag)
        }.vertices.top(100) {
          Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
        }.foreach(t => println(t._2._2 + ": " + t._2._1))*/

    //prGraph.vertices.foreach(println)
  }

}
