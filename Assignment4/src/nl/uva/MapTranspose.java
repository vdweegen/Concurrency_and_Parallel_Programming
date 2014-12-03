/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva;

import java.io.IOException;
import java.util.StringTokenizer;
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
 * This mapper turns columns into rows.
 *
 * @author S. Koulouzis
 */
public class MapTranspose extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

    Log log = LogFactory.getLog(nl.uva.MapTranspose.class);
    private Text columns = new Text();
    private IntWritable emitKey = new IntWritable();

    static enum Counters {

        INPUT_LINES
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> oc, Reporter rprtr) throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        int j = 0;
        while (tokenizer.hasMoreTokens()) {
            emitKey.set(j);
            columns.set(key + "," + tokenizer.nextToken());
            oc.collect(emitKey, columns);
            j++;
            rprtr.setStatus("sending: " + emitKey + " : " + columns);
            rprtr.incrCounter(MapTranspose.Counters.INPUT_LINES, 1);
        }
    }
}
