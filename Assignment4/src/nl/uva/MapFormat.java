/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This mapper reads lines from the result matrix C and emits the key/value pairs 
 * to the reducer. 
 * @author S. Koulouzis
 */
public class MapFormat extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

    Log log = LogFactory.getLog(nl.uva.MapFormat.class);

    static enum Counters {

        INPUT_LINES
    }

    @Override
    public void map(LongWritable k1, Text v1, OutputCollector<IntWritable, Text> oc, Reporter rprtr) throws IOException {
        String[] keyValue = v1.toString().split("\t");
        Integer keyInt = Integer.valueOf(keyValue[0].split(",")[0]);
        IntWritable emitKey = new IntWritable(keyInt);
        Text emitValue = new Text(keyValue[1]);
        oc.collect(emitKey, emitValue);
    }
}
