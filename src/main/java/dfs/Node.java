package dfs;

/**
 *
 */
public class Node {

    String name;

    Node[] children = {};

    Node(String name, Node[] children) {
        this.name = name;
        this.children = children;
    }

    public Node(String name) {
        this.name = name;
    }
}
