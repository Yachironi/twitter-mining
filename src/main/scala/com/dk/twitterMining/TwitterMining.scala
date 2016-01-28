package com.dk.twitterMining

import java.io._
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.SingleGraph
import org.graphstream.graph.{Edge, Graph, Node}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsValue, _}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by yachironi on 15/12/15.
  */
object TwitterMining {

  val maxNumberOfNodes = 30
  val epsilon = 0.1D

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Error:\nPlease specify the arguments as: <input_data_set_path> <output_directory_path>")
      System.exit(1)
    }
    System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
    System.setProperty("sun.java2d.opengl", "True")
    System.setProperty("sun.java2d.directx", "True")
    val input = args(0)
    val dataSetName = (new File(args(0))).getName
    val outputDirectory = args(1) + "/" + dataSetName
    new File(outputDirectory).mkdirs();


    // Apache Spark Configuration
    val conf = new SparkConf().setAppName("WorldCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json(input)
    val dataSetFiltered = df.select("entities.hashtags.text").rdd.map(row => row.getList(0)).filter(r => r.size() > 0).distinct()


    // HashTag , Number of unique tweets, number of unique users
    df.registerTempTable("tweets")
    val hashTagNbTweetsNbUsers: Map[String, Map[String, Int]] = sqlContext.sql("select entities.hashtags.text, id, user.id from tweets")
      .rdd
      .distinct()
      .flatMap(row => row.getList[String](0).map(e => (e.toLowerCase(), row.getLong(1).toString, row.getLong(2).toString)))
      .groupBy(_._1)
      .map(e => (e._1, Map("nb_unique_tweets" -> e._2.groupBy(_._2).size, "nb_unique_users" -> e._2.groupBy(_._3).size)))
      .collect()
      .toMap

    // Creating the co-occurrence graph
    // Map and reducing the nodes in order to see the co-occurence of
    // a Hashtag with another one in the same tweet
    //
    // Key             -> Value
    // (String,String) -> Long
    //
    // The map and reduce are on top of Spark RDD, they are distributed
    //
    val coOccurenceMap = dataSetFiltered.flatMap(
      list => {
        list.toArray().map(e => e.asInstanceOf[String].toLowerCase).flatMap(
          hashtag1 => {
            list.toArray().map(e => e.asInstanceOf[String].toLowerCase).map(hashtag2 => {
              if (hashtag1 != hashtag2) {
                ((hashtag1, hashtag2), 1L)
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
    // coOccurenceMap.top(30) {
    // Ordering.by((entry: Tuple2[(String, String), Long]) => entry._2)
    //}.foreach(edge => {
    // println(edge._1._1 + "--" + edge._2 + "-->" + edge._1._2)
    //})
    //=========================================================================

    val edgesArray: Array[(String, String, String, Long)] = coOccurenceMap
      .map(edge => (edge._1._1 + edge._1._2, edge._1._1, edge._1._2, edge._2))
      .collect()

    val graph = createGraph(edgesArray, false)

    val denseGraphs: mutable.Map[Int, Graph] = getDenseGraphs(graph, 4, epsilon, reduceToMax = true, display = true)

    // Print the densest sub graph
    // denseGraphs.get(0).get.display(true)

    denseGraphs.foreach(densGraph => {
      var fileName = ""
      if (densGraph._1 == 0) {
        fileName = outputDirectory + "/" + dataSetName + ".DENSEST.subgr.json"
      } else {
        fileName = outputDirectory + "/" + dataSetName + "." + densGraph._1 + ".subgr.json"

      }

      val jsonFile: mutable.LinkedHashMap[String, JsValue] = mutable.LinkedHashMap()
      val densGraphList: List[Map[String, String]] = densGraph._2
        .getEdgeSet[Edge]
        .map(edge => Map("edge" -> edge.getId, "source" -> edge.getSourceNode[Node].getId, "destination" -> edge.getTargetNode[Node].getId))
        .toList

      jsonFile("density") = getGraphDensity(densGraph._2).toJson
      jsonFile("nb_unique_users") = numberOfDistinctUsers(densGraph._2, hashTagNbTweetsNbUsers).toJson
      jsonFile("nb_unique_tweets") = numberOfDistinctTweets(densGraph._2, hashTagNbTweetsNbUsers).toJson
      jsonFile("edges") = densGraphList.toJson

      Files.deleteIfExists((new File(fileName)).toPath())
      val writer = new PrintWriter(new File(fileName))
      writer.write(jsonFile.toMap.toJson.prettyPrint)
      writer.close()
    })
  }

  def numberOfDistinctUsers(graph: Graph, mapHashTagUserTweets: Map[String, Map[String, Int]]): Long = {
    var numberOfUsers = 0L
    graph.getNodeSet[Node].foreach(node => {
      numberOfUsers += mapHashTagUserTweets.get(node.getId).get.get("nb_unique_users").get
    })
    return numberOfUsers
  }

  def numberOfDistinctTweets(graph: Graph, mapHashTagUserTweets: Map[String, Map[String, Int]]): Long = {
    var numberOfTweets = 0L
    graph.getNodeSet[Node].foreach(node => {
      numberOfTweets += mapHashTagUserTweets.get(node.getId).get.get("nb_unique_tweets").get
    })
    return numberOfTweets
  }

  def getDenseGraphs(graph: Graph, number: Int, epsilon: Double, reduceToMax: Boolean, display: Boolean): mutable.Map[Int, Graph] = {
    val denseGraphs: mutable.Map[Int, Graph] = scala.collection.mutable.Map[Int, Graph]()

    var i: Int = 0
    val originalGraph = copyGraph(graph)

    while ((i < number) && (originalGraph.getNodeCount > 2)) {

      println(originalGraph.getNodeCount)
      var visualise = false
      if (display & i == 0) {
        visualise = true
      }
      val denseGraph = getDensestSubGraph(originalGraph, visualise, reduceToMax, epsilon)

      // Remove the nodes that apear in the densest sub graph from the original graph
      denseGraph.getNodeSet[Node].foreach(node => {
        originalGraph.removeNode(node.getId).asInstanceOf[Node]
      })
      println(originalGraph.getNodeCount)
      denseGraphs(i) = denseGraph
      i += 1
    }

    return denseGraphs
  }

  def getGraphDensity(graph: Graph): Double = {
    return graph.getEdgeCount.toDouble / graph.getNodeCount.toDouble
  }

  def getDensestSubGraph(graph: Graph, display: Boolean, reduceToMax: Boolean, epsilon: Double): Graph = {
    var iteration: Int = 0
    var densestSubGraph = copyGraph(graph)

    val originalGraph = copyGraph(graph)

    if (display) {
      originalGraph.display(true)
    }

    breakable {
      while (originalGraph.getNodeCount > 1) {
        if (display) {
          Thread.sleep(1000)
        }
        iteration += 1
        println("Iteration:\t" + iteration)

        //val minDegree = graph.getNodeSet.min {
        // Ordering.by((entry: Node) => entry.getDegree)
        //}.asInstanceOf[Node].getDegree
        //val nodesToBeRemoved = graph.getNodeSet[Node].filter(p => (p.getDegree == minDegree))

        val nodesToBeRemoved = originalGraph.getNodeSet[Node].filter(node => (node.getDegree <= (2 * (1 + epsilon) * getGraphDensity(originalGraph))))

        if (getGraphDensity(densestSubGraph) <= getGraphDensity(originalGraph)) {
          densestSubGraph = copyGraph(originalGraph)
        }

        if ((originalGraph.getNodeCount - nodesToBeRemoved.size) < 1) {
          break
        }

        nodesToBeRemoved.foreach(node => {
          originalGraph.removeNode[Node](node.getId)
        })
      }
    }

    // Go back to the last densest sub-graph
    upgradeFirstGraphToSecondGraph(originalGraph, densestSubGraph)
    println("Densest subGraph edges: ")
    originalGraph.getEdgeSet[Edge].toArray().map(raw => raw.asInstanceOf[Edge]).foreach(edge => {
      printf("%-20s %-20s %s\n", edge.getSourceNode[Node].getId, "---", edge.getTargetNode[Node].getId)
      //println(edge.getSourceNode[Node].getId + "\t --- \t" + edge.getTargetNode[Node].getId)
    })

    // Reduce the graph nodes to the maximum nodes number if possible
    if (reduceToMax) {

      breakable {
        while (originalGraph.getNodeCount > maxNumberOfNodes) {
          if (display) {
            Thread.sleep(10)
          }
          iteration += 1
          println("Iteration:\t" + iteration)

          val minDegree = originalGraph.getNodeSet.min {
            Ordering.by((entry: Node) => entry.getDegree)
          }.asInstanceOf[Node].getDegree

          val nodesToBeRemoved = originalGraph.getNodeSet[Node].filter(p => (p.getDegree == minDegree))

          if ((originalGraph.getNodeCount - nodesToBeRemoved.size) < maxNumberOfNodes - 1) {
            break
          }
          println(nodesToBeRemoved.size)
          nodesToBeRemoved.foreach(node => {
            originalGraph.removeNode(node.getId).asInstanceOf[Node]
          })

        }
      }

    }


    println("Densest subGraph nodes: ")
    originalGraph.getNodeSet.foreach(println)
    println("Number of nodes: " + originalGraph.getNodeSet.size())
    println("Number of edges: " + originalGraph.getEdgeSet.size())
    println("Number of iteration: " + iteration)
    println("Density : " + getGraphDensity(graph))

    return originalGraph
  }

  def copyGraph(graph: Graph): Graph = {
    var styleSheet = "graph { padding:  20px; } node { size-mode: fit; shape: rounded-box; fill-color: white; stroke-mode: plain; padding: 3px, 2px;text-size:13;text-style:bold;text-padding: 5px, 4px;}";
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
    var styleSheet = "graph { padding:  20px; } node { size-mode: fit; shape: rounded-box; fill-color: white; stroke-mode: plain; padding: 3px, 2px;text-size:13;text-style:bold;text-padding: 5px, 4px;}";
    var graph: Graph = new SingleGraph("tutorial 1")
    graph.addAttribute("ui.stylesheet", styleSheet)
    graph.setAutoCreate(true)
    graph.setStrict(false)
    if (display) {
      val viewer = graph.display(display)
      viewer.enableXYZfeedback(true)
    }
    edges.foreach(edge => {
      val createdEdge = graph.addEdge[Edge](edge._1, edge._2, edge._3)
      if (createdEdge != null) {
        createdEdge.getSourceNode[Node].addAttribute("label", createdEdge.getSourceNode[Node].getId)
        createdEdge.getSourceNode[Node].addAttribute("ui.color", "0.5")

        createdEdge.getTargetNode[Node].addAttribute("label", createdEdge.getTargetNode[Node].getId)
        createdEdge.getTargetNode[Node].addAttribute("ui.color", "0.5")

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
