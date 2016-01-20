import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.SingleGraph
import org.graphstream.graph.{Edge, Graph, Node}

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

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
      * Key             -> Value
      * (String,String) -> Long
      *
      * The map and reduce are on top of Spark RDD, they are distributed
      */
    val coOccurenceMap = dataSetFiltred.flatMap(
      list => {
        list.toArray().map(e => e.asInstanceOf[String].toLowerCase).flatMap(
          hashtag1 => {
            list.toArray().map(e => e.asInstanceOf[String].toLowerCase).map(hashtag2 => {
              if (hashtag1 != hashtag2) {
                ((hashtag1, hashtag2), 1L)
                //((hashtag2, hashtag1), 1L)
              }
            })
          }
        )
      }
    ).filter(e => e.isInstanceOf[Tuple2[(String, String), Long]])
      .map(e => e.asInstanceOf[Tuple2[(String, String), Long]])
      .reduceByKey((valEdge1, valEdge2) => valEdge1 + valEdge2)

    //=========================================================================
    // Print the co-occurrence Map
    //=========================================================================
    /* coOccurenceMap.top(30) {
       Ordering.by((entry: Tuple2[(String, String), Long]) => entry._2)
     }.foreach(edge => {
       println(edge._1._1 + "--" + edge._2 + "-->" + edge._1._2)
     })*/
    //=========================================================================


    // Id counter generator
    var globalCount = 1L

    /**
      * Extracting the vertexes from the coOccurenceMap
      * ((vertexIdSrc,vertexIdDest),coOccurrences) => Vertex(hashTag,VertexID)
      */
    /* val verticesMap: Map[String, VertexId] = coOccurenceMap.keys
       .flatMap(tuple => Array[String](tuple._1, tuple._2))
       .collect()
       .distinct // we have hashTag1->hashTag2 and hashTag2 -> hashTag1 , here we eliminate the duplicates
       .map(hashtag => {
       globalCount += 1
       (hashtag, globalCount)
     }
     ).toMap


     // Convert the verticesMap to Array of Tuple[(Long, String)]
     val vertexArray = verticesMap.map(vertex => (vertex._2, vertex._1)).toArray[(Long, String)]*/

    /**
      * Extracting the edges from the coOccurenceMap
      * ((vertexIdSrc,vertexIdDest),coOccurrences) => Edge[Long]
      * Edge class contain (srcVertexes , destVertexes)
      */
    /* val edgeArray: Array[Edge[VertexId]] = coOccurenceMap
       .map(edge => Edge(verticesMap.get(edge._1._1).get, verticesMap.get(edge._1._2).get, edge._2))
       .collect()*/

    val edgesArray: Array[(String, String, String, Long)] = coOccurenceMap
      .map(edge => (edge._1._1 + edge._1._2, edge._1._1, edge._1._2, edge._2))
      .collect()

    var graph = createGraph(edgesArray, true)
    graph = getDensestSubGraph(graph)
    println(graph.getNodeCount + " ==> " + graph.getEdgeCount)
    System.out.println("Densest subGraph is: ")
    graph.getEdgeSet.foreach(println)

    /**
      * Constructing the graph, using GraphX library from Spark
      *
      */

    // Parallelize the the verteces and edges, using the Spark FrameWork
    /*val vertices: RDD[(VertexId, String)] = sc.parallelize(vertexArray)
    val edges: RDD[Edge[Long]] = sc.parallelize(edgeArray)
    val graph = Graph(vertices, edges)*/

    // Print graph
    /*  graph.triplets.top(30) {
        Ordering.by(x => x.attr)
      }.foreach(triple => {
        println(triple.srcAttr + "===" + triple.attr + "==>" + triple.dstAttr)
      })


    // Define a class to more clearly model the HashTag property
    case class HashTag(hashTag: String, inDeg: Int, outDeg: Int)

    // Create a hash tag Graph
    val initialHashTagGraph: Graph[HashTag, Long] = graph.mapVertices { case (id, hashTag) => new HashTag(hashTag, 0, 0) }

    // Fill in the degree information

    val originalGraph = initialHashTagGraph.outerJoinVertices(initialHashTagGraph.inDegrees) {
      case (vertexId, hashTag, inDegOpt) => HashTag(hashTag.hashTag, inDegOpt.getOrElse(0), hashTag.outDeg)
    }.outerJoinVertices(initialHashTagGraph.outDegrees) {
      case (vertexId, hashTag, outDegOpt) => HashTag(hashTag.hashTag, hashTag.inDeg, outDegOpt.getOrElse(0))
    }

    var hashTagGraph = initialHashTagGraph.outerJoinVertices(initialHashTagGraph.inDegrees) {
      case (vertexId, hashTag, inDegOpt) => HashTag(hashTag.hashTag, inDegOpt.getOrElse(0), hashTag.outDeg)
    }.outerJoinVertices(initialHashTagGraph.outDegrees) {
      case (vertexId, hashTag, outDegOpt) => HashTag(hashTag.hashTag, hashTag.inDeg, outDegOpt.getOrElse(0))
    }

    var iterationNumber = 0;
    var lastGraph = hashTagGraph
    while (hashTagGraph.vertices.count() > 1) {
      iterationNumber += 1
      lastGraph = hashTagGraph
      println("Iteration \t======> \t" + iterationNumber)
      println("Node numbers \t======> \t" + hashTagGraph.vertices.count())
      println("Edges numbers \t======> \t" + hashTagGraph.triplets.count())
      hashTagGraph = hashTagGraph.outerJoinVertices(hashTagGraph.inDegrees) {
        case (vertexId, hashTag, inDegOpt) => HashTag(hashTag.hashTag, inDegOpt.getOrElse(0), hashTag.outDeg)
      }.outerJoinVertices(initialHashTagGraph.outDegrees) {
        case (vertexId, hashTag, outDegOpt) => HashTag(hashTag.hashTag, hashTag.inDeg, outDegOpt.getOrElse(0))
      }
      val minDgree = hashTagGraph.triplets.min() {
        Ordering.by(x => x.srcAttr.inDeg)
      }.srcAttr.inDeg

      println("Min edge number\t=======> \t " + minDgree)
      hashTagGraph = hashTagGraph.subgraph(triple => ((triple.srcAttr.inDeg != minDgree)), (vertexId, hashtag) => hashtag.inDeg != minDgree)

      val deletedSubGraph = hashTagGraph.subgraph(triple => ((triple.srcAttr.inDeg == minDgree)), (vertexId, hashtag) => hashtag.inDeg == minDgree)

      val neighbors = lastGraph.collectNeighbors(EdgeDirection.Both);
      /*
            deletedSubGraph.vertices.foreach(vertex => {

              neighbors.filter(vertexNeighbors => (vertexNeighbors._1 == vertex._1)).foreach(vertexNeighbors => {

                vertexNeighbors._2.foreach(neighbor => {
                  neighbor._2.inDeg = neighbor._2.inDeg - 1;

                })
              })
      hashTagGraph.collectNeighbors(EdgeDirection.Both).filter(vertex => vertex._1)

    }) */
    }

    lastGraph.triplets.take(30).foreach(triple => {
      println(triple.srcAttr + "===" + triple.attr + "===>" + triple.dstAttr)
    })
 */
    /*
    originalGraph.triplets.foreach(triple => {
      println(triple.srcAttr + "===" + triple.attr + "===>" + triple.dstAttr)
    })*/

    /*for ((id, property) <- hashTagGraph.vertices.collect) {
      println(s"Hash '${property.hashtag}' co-occurred with ${property.inDeg + property.outDeg} hashtags.")
    }*/

    //var vpred = null
    // hashTagGraph.subgraph(vpred = (id, hashtag) => hashtag.inDeg + hashtag.outDeg >= 100).vertices.foreach(println)

    //val prGraph = hashTagGraph.staticPageRank(5).cache

    //val ccGraph = hashTagGraph.connectedComponents()


    /*
    hashTagGraph.triplets.foreach {
      edgeTriplet => println(s"${edgeTriplet.srcAttr.hashtag} ==${edgeTriplet.attr}==> ${edgeTriplet.dstAttr.hashtag}}")
    }*/

    /*
    hashTagGraph.triplets.top(30) {
      Ordering.by((entry: EdgeTriplet[HashTag, Long]) => entry.attr)
    }.foreach(edge => {
      println(edge.srcAttr.hashTag + "--" + edge.attr + "-->" + edge.srcAttr.hashTag)
    })*/


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


  def getDensestSubGraph(graph: Graph): Graph = {
    var iteration: Int = 0
    Thread.sleep(1000)
    breakable {
      while (graph.getNodeCount > 1) {
        Thread.sleep(1000)
        iteration += 1
        System.out.println("Iteration:\t" + iteration)
        System.out.println("Node numbers \t======> \t" + graph.getNodeCount)
        System.out.println("Edges numbers \t======> \t" + graph.getEdgeCount)

        val minDegree = graph.getNodeSet.min {
          Ordering.by((entry: Node) => entry.getDegree)
        }.asInstanceOf[Node].getDegree



        val nodesToBeRemoved = graph.getNodeSet[Node].filter(p => (p.getDegree == minDegree))

        if ((graph.getNodeCount - nodesToBeRemoved.size) < 1) {
          break
        }
        nodesToBeRemoved.foreach(node => {
          graph.removeNode[Node](node)
        })

      }
    }
    return graph
  }

  def createGraph(edges: Array[(String, String, String, Long)], display: Boolean): Graph = {
    var styleSheet = "node {" + "	fill-color: black;" + "}" + "node.marked {" + "	fill-color: red;" + "}";
    var graph: Graph = new SingleGraph("tutorial 1")
    graph.addAttribute("ui.stylesheet", styleSheet)
    graph.setAutoCreate(true)
    graph.setStrict(false)
    graph.display(display)

    edges.foreach(edge => {
      println("=============> " + graph.getEdge(edge._1) + " <=============")
      val createdEdge = graph.addEdge[Edge](edge._1, edge._2, edge._3)
      if (createdEdge != null) {
        createdEdge.getSourceNode[Node].addAttribute("ui.label", createdEdge.getSourceNode[Node].getId)
        createdEdge.getTargetNode[Node].addAttribute("ui.label", createdEdge.getTargetNode[Node].getId)
      }
    })

    return graph
  }

}
