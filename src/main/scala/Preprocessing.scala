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

    val edgesArray: Array[(String, String, String, Long)] = coOccurenceMap
      .map(edge => (edge._1._1 + edge._1._2, edge._1._1, edge._1._2, edge._2))
      .collect()

    var graph = createGraph(edgesArray, true)
    val epsilon = 0.1D

    graph = getDensestSubGraph(graph, epsilon)

  }

  def getGraphDensity(graph: Graph): Double = {
    return graph.getEdgeCount.toDouble / graph.getNodeCount.toDouble
  }

  def getDensestSubGraph(graph: Graph, epsilon: Double): Graph = {
    var iteration: Int = 0
    var densestSubGraph = copyGraph(graph)

    breakable {
      while (graph.getNodeCount > 1) {
        Thread.sleep(100)
        iteration += 1
        println("Iteration:\t" + iteration)

        //val minDegree = graph.getNodeSet.min {
        // Ordering.by((entry: Node) => entry.getDegree)
        //}.asInstanceOf[Node].getDegree
        //val nodesToBeRemoved = graph.getNodeSet[Node].filter(p => (p.getDegree == minDegree))

        val nodesToBeRemoved = graph.getNodeSet[Node].filter(node => (node.getDegree <= (2 * (1 + epsilon) * getGraphDensity(graph))))

        if (getGraphDensity(densestSubGraph) <= getGraphDensity(graph)) {
          densestSubGraph = copyGraph(graph)
        }

        if ((graph.getNodeCount - nodesToBeRemoved.size) < 1) {
          break
        }

        nodesToBeRemoved.foreach(node => {
          graph.removeNode[Node](node)
        })
      }
    }

    // Go back to the last densest sub-graph
    upgradeFirstGraphToSecondGraph(graph, densestSubGraph)
    println("Densest subGraph edges: ")
    graph.getEdgeSet[Edge].toArray().map(raw => raw.asInstanceOf[Edge]).foreach(edge => {
      printf("%-20s %-20s %s\n", edge.getSourceNode[Node].getId, "---", edge.getTargetNode[Node].getId)
      //println(edge.getSourceNode[Node].getId + "\t --- \t" + edge.getTargetNode[Node].getId)
    })
    println("Densest subGraph nodes: ")
    graph.getNodeSet.foreach(println)
    println("Number of nodes: " + graph.getNodeSet.size())
    println("Number of edges: " + graph.getEdgeSet.size())
    println("Number of iteration: " + iteration)
    println("Density : " + getGraphDensity(graph))

    return graph
  }

  def copyGraph(graph: Graph): Graph = {
    var styleSheet = "node {" + "	fill-color: black;" + "}" + "node.marked {" + "	fill-color: red;" + "}";
    var graphCopy: Graph = new SingleGraph("tutorial 5")
    graphCopy.addAttribute("ui.stylesheet", styleSheet)
    graphCopy.setAutoCreate(true)
    graphCopy.setStrict(false)

    graph.getEdgeSet[Edge].toArray().map(e => e.asInstanceOf[Edge]).foreach(edge => {
      val createdEdge = graphCopy.addEdge[Edge](edge.getId, edge.getSourceNode[Node].getId, edge.getTargetNode[Node].getId)
      if (createdEdge != null) {
        // Copying the edge attributes
        edge.getAttributeKeySet.foreach(attribute => {
          createdEdge.setAttribute(attribute, edge.getAttribute(attribute))
        })

        // Copying the source node attributes
        edge.getSourceNode[Node].getAttributeKeySet.foreach(attribute => {
          createdEdge.getSourceNode[Node].addAttribute(attribute, edge.getSourceNode[Node].getAttribute(attribute))
        })

        // Copying the target node attributes
        edge.getTargetNode[Node].getAttributeKeySet.foreach(attribute => {
          createdEdge.getTargetNode[Node].addAttribute(attribute, edge.getTargetNode[Node].getAttribute(attribute))
        })
      }
    }
    )

    return graphCopy;
  }

  def createGraph(edges: Array[(String, String, String, Long)], display: Boolean): Graph = {
    var styleSheet = "node {" + "	fill-color: black;" + "}" + "node.marked {" + "	fill-color: red;" + "}";
    var graph: Graph = new SingleGraph("tutorial 1")
    graph.addAttribute("ui.stylesheet", styleSheet)
    graph.setAutoCreate(true)
    graph.setStrict(false)
    graph.display(display)

    edges.foreach(edge => {
      val createdEdge = graph.addEdge[Edge](edge._1, edge._2, edge._3)
      if (createdEdge != null) {
        createdEdge.getSourceNode[Node].addAttribute("ui.label", createdEdge.getSourceNode[Node].getId)
        createdEdge.getTargetNode[Node].addAttribute("ui.label", createdEdge.getTargetNode[Node].getId)
      }
    })

    return graph
  }

  def upgradeFirstGraphToSecondGraph(graph1: Graph, graph2: Graph) = {
    val edgesToBeRemoved: Array[Edge] = Array()
    graph1.getEdgeSet[Edge].toArray().map(e => e.asInstanceOf[Edge]).foreach(edge => {
      if (graph2.getEdge(edge.getId) == null) {
        edgesToBeRemoved :+ edge
      }
    })
    edgesToBeRemoved.foreach(edge => {
      graph1.removeEdge(edge.getId)
    })
    graph2.getEdgeSet[Edge].toArray().map(e => e.asInstanceOf[Edge]).foreach(edge => {
      val createdEdge = graph1.addEdge[Edge](edge.getId, edge.getSourceNode[Node].getId, edge.getTargetNode[Node].getId)

      if (createdEdge != null) {
        // Copying the edge attributes
        edge.getAttributeKeySet.foreach(attribute => {
          createdEdge.setAttribute(attribute, edge.getAttribute(attribute))
        })

        // Copying the source node attributes
        edge.getSourceNode[Node].getAttributeKeySet.foreach(attribute => {
          createdEdge.getSourceNode[Node].addAttribute(attribute, edge.getSourceNode[Node].getAttribute(attribute))
        })

        // Copying the target node attributes
        edge.getTargetNode[Node].getAttributeKeySet.foreach(attribute => {
          createdEdge.getTargetNode[Node].addAttribute(attribute, edge.getTargetNode[Node].getAttribute(attribute))
        })
      }
    })
  }


}
