package dfs;

/**
 *
 */
public class DepthFirstSearch {

    public static void main(String... args) {

        Node start = createGraph();



    }



    static Node createGraph() {
        Node n4 = new Node("4");
        Node n5 = new Node("5");
        Node n3 = new Node("3", new Node[]{n4, n5});

        Node n6 = new Node("n6");
        Node n2 = new Node("n2", new Node[]{n3, n6});

        Node n10 = new Node("10");
        Node n11 = new Node("11");
        Node n9 = new Node("9", new Node[]{n10, n11});

        Node n12 = new Node("12");

        Node n8 = new Node("8", new Node[]{n9, n12});
        Node n7 = new Node("7");

        Node n1 = new Node("1", new Node[]{n2, n7, n8});
        return n1;
    }

}
