import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import sun.misc.Queue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCrawler {

    public static void main(String args[]) throws InterruptedException, IOException {
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("C:\\Users\\Robert\\Documents\\Neo4j\\try3.graphdb");
        registerShutdownHook(graphDb);

        // timeout connection after 500 miliseconds
        System.setProperty("sun.net.client.defaultConnectTimeout", "1000");
        System.setProperty("sun.net.client.defaultReadTimeout", "1000");

        // initial web page
        String s = "https://www.facebook.com";
        //TODO serialize queue and use it to save the state of the search
        // list of web pages to be examined
        Queue<String> q = new Queue<String>();
        q.enqueue(s);

        // existence symbol table of examined web pages
        List<String> list = new LinkedList<String>();
        Map<String, Integer> map = new HashMap<>();
        //list.add(s);
        String regexp = "(http|https):\\/\\/(\\w+\\.)*(\\w+)";
        Pattern pattern = Pattern.compile(regexp);
//            Index<Node> links = index.forNodes("Links" );
        // breadth first search crawl of web
        while (!q.isEmpty()) {
            String v = q.dequeue();
            System.out.println(v + " Map length: " + map.size());

            URL oracle = new URL(v);
            Transaction tx = graphDb.beginTx();
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(oracle.openStream()));
                if (in != null) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        Matcher matcher = pattern.matcher(inputLine);
                        while (matcher.find()) {
                            String w = matcher.group();
                            if (map.containsKey(w)) {
                                map.put(w, map.get(w) + 1);
//                                System.out.println(w);
//                                list.add(w);
                            } else {
                                map.put(w, 1);
                                q.enqueue(w);

                                // Database operations go here

                                Node firstNode;
                                Node secondNode;
                                Relationship relationship;
                                firstNode = null;
//                                    firstNode.setProperty( "link", v);
                                secondNode = null;
//                                    secondNode.setProperty( "link", w);

//                                    relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
//                                    relationship.setProperty( "message", "knows" );

                                ExecutionEngine engine = new ExecutionEngine(graphDb);

                                ExecutionResult result;
                                result = engine.execute("match n where n.link = \"" + v + "\"return n");
                                Iterator<Node> n_column = result.columnAs("n");

                                for (Node node : IteratorUtil.asIterable(n_column)) {
                                    firstNode = node;
                                    firstNode.setProperty("count", (Integer) node.getProperty("count") + 1);
                                }

                                result = engine.execute("match n where n.link = \"" + w + "\"return n");
                                n_column = result.columnAs("n");

                                for (Node node : IteratorUtil.asIterable(n_column)) {
                                    secondNode = node;
                                    secondNode.setProperty("count", (Integer) node.getProperty("count") + 1);
                                }
                                if(firstNode == null){
                                    firstNode = graphDb.createNode();
                                }

                                if(secondNode == null){
                                    secondNode = graphDb.createNode();
                                }

                                if (firstNode.getPropertyKeys().toString().equals("[]")) {
                                    firstNode.setProperty("link", v);
                                    firstNode.setProperty("count", 1);
                                }
                                if (secondNode.getPropertyKeys().toString().equals("[]")) {
                                    secondNode.setProperty("link", w);
                                    secondNode.setProperty("count", 1);
                                }
                                relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
                                relationship.setProperty("message", "knows");
                                tx.success();

                            }

                        }
                    }
                }
                in.close();
//                    graphDb.shutdown();
            } catch (Exception e) {
                System.out.println("Exception " + e.toString());
            } finally {
                tx.close();
            }
        }

    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    private static enum RelTypes implements RelationshipType {
        KNOWS
    }
}
