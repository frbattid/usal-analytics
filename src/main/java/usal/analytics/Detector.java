/*
 * Some header..
 */
package usal.analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;
import usal.spark.functions.AvroDeserializer;
import usal.spark.functions.HashMapDetector;
import usal.spark.functions.NGSITuple;
import usal.spark.functions.NGSITuplesAggregatorAll;
import usal.spark.functions.NGSITuplesAggregatorByKey;
import usal.spark.functions.NGSITuplesExtractor;
import usal.spark.functions.NGSITupleToPair;

/**
 * Detector application.
 * 
 * @author frbattid
 */
public final class Detector {
    
    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private Detector() {
    } // Detector
    
    /**
     * Main class for this detector application.
     * @param args
     */
    public static void main(String[] args) {
        // Show usage
        if (args.length != 4) {
            System.err.println("Usage: usal.UsalAggregator <host> <port> <models_base_path> <debug>");
            System.exit(1);
        } // if

        // Get parameters
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String modelsBasePath = args[2];
        boolean debug = args[3].equals("true");

        // Batch interval duration (1 minute)
        Duration batchInterval = new Duration(60000);

        // Create the streaming context
        SparkConf sparkConf = new SparkConf().setAppName("Detector");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        // Create the stream object from Avro binaries sent by Flume
        JavaReceiverInputDStream<SparkFlumeEvent> stream = FlumeUtils.createStream(ssc, host, port);

        // Get string-based events
        JavaDStream<String> streamStr = stream.map(new AvroDeserializer());
        
        if (debug) {
            streamStr.print();
        } // if

        // Get NGSI tuples
        JavaDStream<Tuple2<NGSITuple, Float>> tuples = streamStr.flatMap(new NGSITuplesExtractor());
        
        if (debug) {
            tuples.print();
        } // if
        
        // Sum all the NGSI tuples
        JavaDStream<Tuple2<NGSITuple, Float>> sumAll =
                tuples.reduce(new NGSITuplesAggregatorAll("aggregated", "model", "", ""));
        
        if (debug) {
            sumAll.print();
        } // if
        
        // Read the model for the entire neighbourhood and compare it with the current data
        JavaPairDStream<String, Float> sumAllAsPairs = sumAll.mapToPair(new NGSITupleToPair(true));
        sumAllAsPairs.toJavaDStream().foreachRDD(new HashMapDetector(modelsBasePath, 24, true));
        
        // Translate the NGSI tuples to a <String, Float> pair
        JavaPairDStream<String, Float> pairs = tuples.mapToPair(new NGSITupleToPair(true));
        
        if (debug) {
            pairs.print();
        } // if
        
        // Sum the <String, Float> pairs by key
        JavaPairDStream<String, Float> sumByKeyAsPairs = pairs.reduceByKey(new NGSITuplesAggregatorByKey());
        
        if (debug) {
            sumByKeyAsPairs.print();
        } // if
        
        // Read the model for all the homes and compare them with the current data
        sumByKeyAsPairs.toJavaDStream().foreachRDD(new HashMapDetector(modelsBasePath, 24, true));
        
        // Start the app
        ssc.start();
        System.out.println("Detector started, ready to receive Avro events from Cygnus/Flume...");
        ssc.awaitTermination();
    } // main
    
} // Detector
