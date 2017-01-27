/*
 * Some header...
 */
package usal.spark.functions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 *
 * @author frbattid
 */
public class HashMapDetector implements VoidFunction<
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
    public HashMapDetector(String modelsBasePath, int numSlots, boolean debug) {
        this.modelsBasePath = modelsBasePath;
        this.numSlots = numSlots;
        this.debug = debug;
        this.models = new HashMap<>();
    } // HashMapDetector

    @Override
    public void call(JavaRDD<Tuple2<String, Float>> rdd) {
        List<Tuple2<String, Float>> list = rdd.collect();

        for (Tuple2<String, Float> tuple : list) {
            Calendar calendar = GregorianCalendar.getInstance();
            calendar.setTime(new Date());
            String modelName = tuple._1();
            Float streamValue = tuple._2();
            HashMap<Integer, Float> model = models.get(modelName);
            
            if (model == null) {
                model = new HashMap<>(24);
                models.put(modelName, model);
                
                try {
                    read(model, modelName);
                } catch (IOException e) {
                    System.err.println("There was a problem while reading the model. Details: " + e.getMessage());
                } // try catch
            } // if
            
            Float modelValue = model.get(calendar.get(Calendar.MINUTE) % numSlots);
            
            if (streamValue > modelValue) {
                System.out.println("Anomaly detected, stream-based value (" + tuple._2()
                        + ") is greater than the modeled one (" + modelValue + ")");
            } // if
        } // for
    } // call
    
    private void read(HashMap<Integer, Float> model, String modelName) throws IOException {
        if (debug) {
            System.out.println("Reading " + modelsBasePath + "/" + modelName);
        } // if
        
        Properties properties = new Properties();
        File file = new File(modelsBasePath + "/" + modelName);
        
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            properties.load(fileInputStream);
        } // try

        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            model.put(Integer.valueOf(key), Float.valueOf(value));
        } // for
    } // modelFile
    
} // HashMapDetector
