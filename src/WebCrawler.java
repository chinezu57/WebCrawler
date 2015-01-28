import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCrawler {

    public static void main(String args[]) throws InterruptedException, IOException {
        //TODO multithreading the search
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("crawler.graphdb");
        registerShutdownHook(graphDb);

        // timeout connection after 500 miliseconds
        System.setProperty("sun.net.client.defaultConnectTimeout", "1000");
        System.setProperty("sun.net.client.defaultReadTimeout", "1000");

        // initial web page
        //TODO get the link from the arguments of the jar
        String s = "https://www.google.ro/";
        //TODO serialize queue and use it to save the state of the search
        //TODO find a way to run some code when you force  quit the program (the queue serialize part)
        //TODO when the program closes serialize the queue so we can continue the crawl from where we left off

        Queue<String> q = new LinkedList<String>();
        q.add(s);

        // existence symbol table of examined web pages
        List<String> list = new LinkedList<String>();
        //TODO query the database to see if we already have visited a link don't store them in a map locally (uses to much ram ex:8000 entries in map and unknown in queue = 3GB)
        //list.add(s);
        //TODO find a better regex for url matching
        String regexp = "(http|https):\\/\\/(\\w+\\.)*(\\w+)";
        Pattern pattern = Pattern.compile(regexp);
//            Index<Node> links = index.forNodes("Links" );
        // breadth first search crawl of web
        while (!q.isEmpty()) {
            String v = q.remove();
            System.out.println(v + " Queue size: " + q.size());

            URL oracle = new URL(v);
            Transaction tx = graphDb.beginTx();
            try {
                //TODO do not add the node if the link isn't correct (it threw an error)
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(oracle.openStream()));
                if (in != null) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        Matcher matcher = pattern.matcher(inputLine);
                        while (matcher.find()) {
                            String w = matcher.group();
                            if (list.contains(w)) {
//                                map.put(w, map.get(w) + 1);
//                                list.add(w);
//                                System.out.println(w);
//                                list.add(w);
                            } else {
//                                map.put(w, 1);
                                list.add(w);
                                q.add(w);
                                //TODO we need to link backwards as well (atm v->w) we need (w->v) as well
                                // Database operations go here

                                Node firstNode;
                                Node secondNode;
                                Relationship relationship;

                                firstNode = null;
                                secondNode = null;

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
                                if (firstNode == null) {
                                    firstNode = graphDb.createNode();
                                }

                                if (secondNode == null) {
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
                //TODO we get quite a few errors here log all of them or at least the most important ones
//                System.out.println("Exception " + e.toString());
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
