import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

/**
 * Created by yachironi on 15/12/15.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        Graph graph = new SingleGraph("Tutorial 1");

        String styleSheet = "graph { padding: 40px; } node { text-alignment: at-right; text-padding: 3px, 2px; text-background-mode: rounded-box; text-background-color: #EB2; text-color: #222; }";
        graph.addAttribute("stylesheet", styleSheet);

        graph.display(true);


        int id = 0;
        for (int i = 0; i < 1; i++) {
            int iId = id++;
            Node x = graph.addNode(String.valueOf(iId));
            x.addAttribute("ui.label", iId);

            graph.getNode(String.valueOf(iId)).addAttribute("Degree", 1L);

            for (int j = 2; j < 23; j++) {
                int jId = id++;
                Node x2 = graph.addNode(String.valueOf(jId));
                x2.addAttribute("ui.label", jId);
                graph.getNode(String.valueOf(jId)).addAttribute("Degree", 1L);

                graph.addEdge(iId + "->" + jId, String.valueOf(iId), String.valueOf(jId));

                graph.getNode(String.valueOf(iId)).setAttribute("Degree", graph.getNode(String.valueOf(iId)).getAttribute("Degree", Long.class) + 1);
                Thread.sleep(100);
            }
        }

        System.out.println(graph.getNode("0").getDegree());

        for (int i = 4; i < 19; i++) {
            graph.removeNode(String.valueOf(i));
            Thread.sleep(100);

        }
        System.out.println(graph.getNode("0").getDegree());

        System.out.println(graph.getNode("0").getAttribute("Degree", Long.class));
    }


}
