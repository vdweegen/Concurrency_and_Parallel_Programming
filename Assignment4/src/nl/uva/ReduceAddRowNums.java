
package nl.uva;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

/**
 * This reducer receives values grouped by key attaches the row number as key 
 * and selects the correct output for each line. Since the number of mappers is 
 * also controlled by the number of input files we generate as many output files 
 * as we want mappers. 
 *
 * @author S. Koulouzis
 */
public class ReduceAddRowNums extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    Log log = LogFactory.getLog(nl.uva.ReduceAddRowNums.class);
    private MultipleOutputs mo;
    private int numberOfOutA;
    private int numberOfOutB;
    private int numOfRowsInC;
    private int numOfColumnsInC;

    static enum Counters {

        OUTPUT_LINES
    }

    @Override
    public void configure(final JobConf job) {
        mo = new MultipleOutputs(job);
        numberOfOutA = job.getInt("number.of.out.A", 1);
        numberOfOutB = job.getInt("number.of.out.B", 1);
        numOfRowsInC = job.getInt("rows.in.matrixC", 1);
        numOfColumnsInC = job.getInt("columns.in.matrixC", 1);
    }

    @Override
    public void reduce(Text key, Iterator<Text> itrtr, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        String matrixName = key.toString().split(",")[0];

        int row = 0;
        int size = 1;
        int left = 1;
        while (itrtr.hasNext()) {
            Text elem = itrtr.next();
            Text keyOut = new Text(Long.toString(row));
            if (matrixName.equals("A")) {
                size = numOfRowsInC / numberOfOutA;
                left = numOfRowsInC % numberOfOutA;
            } else if (matrixName.equals("B")) {
                size = numOfColumnsInC / numberOfOutB;
                left = numOfColumnsInC % numberOfOutB;
            }
            int index = row / (size + left);
            mo.getCollector(matrixName + index, matrixName + index, rprtr).collect(keyOut, elem);
            rprtr.incrCounter(ReduceAddRowNums.Counters.OUTPUT_LINES, 1);
            rprtr.setStatus("Added line number in "+matrixName + index+" output");
            row++;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        mo.close();
    }
}
