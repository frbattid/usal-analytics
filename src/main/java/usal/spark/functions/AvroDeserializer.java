/**
 * Copyright 2017 Francisco Romero Bueno
 *
 * usal-analytics is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * usal-analytics is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with usal-analytics. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please raise an issue at
 * https://github.com/frbattid/usal-analytics
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