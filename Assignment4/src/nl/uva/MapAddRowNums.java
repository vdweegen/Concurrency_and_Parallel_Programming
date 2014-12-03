/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.uva;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This mapper adds line numbers to the input files with the help of
 * MatrixKeyComparator and MatrixOutputValueGroupingComparator. We need this so
 * during the multiplication we know which rows / columns we process
 *
 * @author S. Koulouzis
 */
public class MapAddRowNums extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    Log log = LogFactory.getLog(nl.uva.MapAddRowNums.class);
    private String matrixName;
    private long mapInputStart;

    static enum Counters {

        INPUT_LINES
    }

    @Override
    public void configure(JobConf conf) {
        String[] fileNameTokens = conf.get("map.input.file").split("/");
        String tmp = fileNameTokens[fileNameTokens.length - 1];
        if (tmp.startsWith("B")) {
            matrixName = "B";
        } else if (tmp.startsWith("A")) {
            matrixName = "A";
        } else {
            matrixName = tmp.substring(tmp.length() - 1, tmp.length());
        }
        mapInputStart = conf.getLong("map.input.start", 0);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        Text emitKey = new Text(matrixName + "," + mapInputStart + "," + key.toString());
        rprtr.setStatus("Emitting: " + emitKey + " : " + value);
        rprtr.incrCounter(MapAddRowNums.Counters.INPUT_LINES, 1);
        oc.collect(emitKey, value);
    }
}
