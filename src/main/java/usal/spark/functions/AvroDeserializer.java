/*
 * Some header...
 */
package usal.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 * Deserializes avro binary data, getting a string.
 * 
 * @author frbattid
 */
public class AvroDeserializer implements Function<
       SparkFlumeEvent,
       String> {

    @Override
    public String call(SparkFlumeEvent flumeEvent) {
        String flumeEventStr = flumeEvent.event().toString();
        String ngsiStr = flumeEventStr.substring(
                flumeEventStr.indexOf("bytes") + 9,
                flumeEventStr.length() - 3);
        return ngsiStr;
    } // call

} // AvroDeserializer