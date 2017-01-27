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

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 *
 * @author frbattid
 */
public class HashMapModeler implements VoidFunction<
        JavaRDD<Tuple2<String, Float>>> {
    
    private final String modelsBasePath;
    private final int numSlots;
    private final boolean debug;
    private final HashMap<String, HashMap<Integer, Float>> models;

    /**
     * Constructor.
     * @param modelsBasePath
     * @param debug
     * @param numSlots
     */
    public HashMapModeler(String modelsBasePath, int numSlots, boolean debug) {
        this.modelsBasePath = modelsBasePath;
        this.numSlots = numSlots;
        this.debug = debug;
        this.models = new HashMap<>();
    } // HashMapModeler

    @Override
    public void call(JavaRDD<Tuple2<String, Float>> rdd) {
        List<Tuple2<String, Float>> list = rdd.collect();

        for (Tuple2<String, Float> tuple : list) {
            Calendar calendar = GregorianCalendar.getInstance();
            calendar.setTime(new Date());
            String modelName = tuple._1();
            Float modelValue = tuple._2();
            HashMap<Integer, Float> model = models.get(modelName);
            
            if (model == null) {
                model = new HashMap<>(24);
                models.put(modelName, model);
            } // if
            
            model.put(calendar.get(Calendar.MINUTE) % numSlots, modelValue);
            
            try {
                save(model, modelName);
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                System.err.println("There was some error when saving the model. Details: " + e.getMessage());
            } // try catch
        } // for
    } // call

    private void save(HashMap<Integer, Float> model, String modelName)
        throws FileNotFoundException, UnsupportedEncodingException {
        if (debug) {
            System.out.println("Saving model in " + modelsBasePath + "/" + modelName);
        } // if
        
        try (PrintWriter writer = new PrintWriter(modelsBasePath + "/" + modelName, "UTF-8")) {
            for (int i = 0; i < numSlots; i++) {
                Float value = model.get(i);
                
                if (value == null) {
                    writer.println(i + "=0");
                    
                    if (debug) {
                        System.out.println("Saving " + i + "=0");
                    } // if
                } else {
                    writer.println(i + "=" + model.get(i));
                    
                    if (debug) {
                        System.out.println("Saving " + i + "=" + model.get(i));
                    } // if
                } // if else
            } // for
        } // try
    } // save
    
} // HashMapModeler
