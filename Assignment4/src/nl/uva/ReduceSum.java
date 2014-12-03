package nl.uva;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This reducer should calculate the sums for each row in matrix
  C. 
 * @author S. Koulouzis
 */
public class ReduceSum extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    Log log = LogFactory.getLog(nl.uva.ReduceSum.class);
    private String rowNum;
    private String columnNum;
    private String elementValue;
    private Text value = new Text();
    private Text key = new Text();
    private int x;
    private int y;
    
    static enum Counters {
        OUTPUT_LINES
    }

    @Override
    public void reduce(Text k2, Iterator<Text> itrtr, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        /* Initialise Double to hold results */
    	Double result = Double.valueOf(0);
        while (itrtr.hasNext()) {
        	/* Parse Coordinates */
        	x = Integer.parseInt(k2.toString().split(",")[0]);
            y = Integer.parseInt(k2.toString().split(",")[1]);
            /* Save result */
        	result += Double.valueOf(itrtr.next().toString());
        }
        
        //Here you have to calculate the rowNum and columnNum to use them as a key
        rowNum = String.valueOf(x);
        columnNum = String.valueOf(y);
        
        /* Collect output */
        value.set(columnNum + "," + result.toString());
        key.set(rowNum);
        oc.collect(k2, value);
    }
}
