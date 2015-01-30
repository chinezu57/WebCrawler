package org.pasteur40.crawler;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Application {
    private final BlockingQueue<String> queue;
    private final BlockingQueue<String> list;
    private static GraphDatabaseService graphDb;

    public Application() {
        queue = new LinkedBlockingQueue<>(1000000);
        list = new LinkedBlockingQueue<>(1000000);
    }

    public static void main(String[] args) {
        Application pc = new Application();
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("crawler.graphdb");
        pc.init();
    }

    private void init() {
        long t1 = System.currentTimeMillis();
        queue.add("http://www.aliexpress.com/store/product/skywalker-X8-wing-EPO-Black-1-3-day-delivery/118500_743489292.html");
        ThreadPool pool = new ThreadPool(256);       //thread pool sets the number of workers

        for (int i = 0; i < 1000000; i++) {
            pool.addTask(new Crawler(graphDb, list, queue));            //in the pool we add the tasks that should be run and how many times
        }
        pool.shutdown();
//        try {
//            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("array.txt"));
//            out.writeObject(list);
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        System.out.println("It took :" + (System.currentTimeMillis() - t1) / 1000 + " seconds to run");
    }
}