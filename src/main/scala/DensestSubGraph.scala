import org.graphstream.graph.implementations.SingleGraph
import org.graphstream.graph.{Edge, Graph, Node}

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

/**
  * Created by yachironi on 20/01/16.
  */
object DensestSubGraph {

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
      val createdEdge = graph.addEdge[Edge](edge._1, edge._2, edge._3)
      createdEdge.getSourceNode[Node].addAttribute("ui.label", createdEdge.getSourceNode[Node].getId)
      createdEdge.getTargetNode[Node].addAttribute("ui.label", createdEdge.getTargetNode[Node].getId)
    })

    return graph
  }


  def main(args: Array[String]) {

    val edgesArray: Array[(String, String, String, Long)] = Array(
      ("AB", "A", "B", 1L),
      ("BC", "B", "C", 1L),
      ("CA", "C", "A", 1L),
      ("AD", "A", "D", 1L),
      ("ID", "I", "D", 1L),
      ("IF", "I", "F", 1L),
      ("IA", "I", "A", 1L),
      ("EF", "E", "F", 1L),
      ("EI", "E", "I", 1L),
      ("CI", "C", "I", 1L),
      ("CD", "C", "D", 1L),
      ("BD", "B", "D", 1L)
    )

    var graph = createGraph(edgesArray, true)
    graph = getDensestSubGraph(graph)
    println(graph.getNodeCount + " ==> " + graph.getEdgeCount)
    System.out.println("Densest subGraph is: ")
    graph.getEdgeSet.foreach(println)
  }
}
