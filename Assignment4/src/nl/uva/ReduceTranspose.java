/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Collects the values that are grouped by column number sorts them and saves 
 * them in lines
 * @author S. Koulouzis
 */
public class ReduceTranspose extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

    Log log = LogFactory.getLog(nl.uva.ReduceTranspose.class);

    @Override
    public void reduce(IntWritable key, Iterator<Text> itrtr, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();
        while (itrtr.hasNext()) {
            Text val = itrtr.next();
            String[] indexedValue = val.toString().split(",");
            int index = Integer.valueOf(indexedValue[0]);
            String value = indexedValue[1];
            map.put(index, value);
        }
        
        StringBuilder sb = new StringBuilder();
        Set<Integer> keys = map.keySet();
        for (Integer mKey : keys) {
            String val = map.get(mKey);
            if (sb.length() >= 1) {
                sb.append(" ");
            }
            sb.append(val.toString());
        }
        rprtr.setStatus("saving: " + sb.toString());
        Text value = new Text(sb.toString());
        oc.collect(null, value);
    }
}
