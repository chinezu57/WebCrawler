package org.pasteur40.crawler;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler implements Runnable {
    GraphDatabaseService graphDb;
    BlockingQueue<String> list;
    BlockingQueue<String> queue;

    public Crawler(GraphDatabaseService graphDb, BlockingQueue<String> list, BlockingQueue<String> queue) {
        this.graphDb = graphDb;
        this.list = list;
        this.queue = queue;
    }

    @Override
    public void run() {
        Transaction tx = graphDb.beginTx();
        ExecutionEngine engine = new ExecutionEngine(graphDb);
        try {
            String regexp = "(http|https):\\/\\/(\\w+\\.)*(\\w+)";
            Pattern pattern = Pattern.compile(regexp);
            String url = queue.take();
            list.add(url);
//                System.out.println(url);
            URL oracle = new URL(url);
            //TODO do not add the node if the link isn't correct (it threw an error)
            BufferedReader in = new BufferedReader(new InputStreamReader(oracle.openStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                Matcher matcher = pattern.matcher(inputLine);
                while (matcher.find()) {
                    String w = matcher.group();
                    if (!list.contains(w)) {
                        queue.add(w);
                        //TODO we need to link backwards as well (atm v->w) we need (w->v) as well
                        // Database operations go here
                        //TODO  all database operations
                        Node firstNode;
                        Node secondNode;
                        Relationship relationship;

                        firstNode = null;
                        secondNode = null;


                        ExecutionResult result;
                        result = engine.execute("match n where n.link = \"" + url + "\"return n");
                        Iterator<Node> n_column = result.columnAs("n");

                        for (Node node : IteratorUtil.asIterable(n_column)) {
                            firstNode = node;
//                                    firstNode.setProperty("count", (Integer) node.getProperty("count") + 1);
                        }

                        result = engine.execute("match n where n.link = \"" + w + "\"return n");
                        n_column = result.columnAs("n");

                        for (Node node : IteratorUtil.asIterable(n_column)) {
                            secondNode = node;
//                                    secondNode.setProperty("count", (Integer) node.getProperty("count") + 1);
                        }
                        if (firstNode == null) {
                            firstNode = graphDb.createNode();
                        }

                        if (secondNode == null) {
                            secondNode = graphDb.createNode();
                        }

                        if (firstNode.getPropertyKeys().toString().equals("[]")) {
                            firstNode.setProperty("link", url);
//                                    firstNode.setProperty("count", 1);
                        }
                        if (secondNode.getPropertyKeys().toString().equals("[]")) {
                            secondNode.setProperty("link", w);
//                                    secondNode.setProperty("count", 1);
                        }
                        Boolean connected = false;
                        for (Relationship r : firstNode.getRelationships()) {
                            if (r.getOtherNode(firstNode).equals(secondNode)) {
                                connected = true;
                            }
                        }

                        if (!connected) {
                            relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
                            relationship.setProperty("message", "knows");
                        }
                        tx.success();
                    }
                }
            }

            in.close();
            System.out.println("Number of visited sites : " + list.size());
        } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            //TODO we get quite a few errors here log all of them or at least the most important ones
//                System.out.println("Exception " + e.toString());
        } finally {
            tx.close();
        }
//            System.out.println(engine.execute("match n return count(n)").dumpToString());
    }
}