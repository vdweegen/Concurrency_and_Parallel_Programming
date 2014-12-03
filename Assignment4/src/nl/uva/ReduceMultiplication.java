package nl.uva;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.util.HashMap;
import java.util.Map;


/**
 * This reducer is performing the actual multiplication between rows and
 * columns. As long as the mapper is given the correct keys, the correct rows
 * and columns will be grouped together.
 *
 * @author S. Koulouzis
 */
public class ReduceMultiplication extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    Log log = LogFactory.getLog(ReduceMultiplication.class);
    Text emitKey = new Text();
    Text emitValue = new Text();
    
    static enum Counters {

        RESAULT
    }

    @Override
    public void reduce(Text key, Iterator<Text> valueItrtr, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {
        int index = 0;
        int numOfMul = 0;
        int x = 0;
        int y = 0;
        HashMap map = new HashMap(); /* Allow Duplicates */

        //The rows and columns will be grouped on the same keys. 
        //You have to split  them
        while (valueItrtr.hasNext()) {
            x = Integer.parseInt(key.toString().split(",")[0]);
            y = Integer.parseInt(key.toString().split(",")[1]);
            numOfMul = Integer.parseInt(key.toString().split(",")[2]);
            map.put(key.toString().split(",")[0] + "," + key.toString().split(",")[1] + "," + index, valueItrtr.next().toString());
            index++;
        }
        
        Double result = Double.valueOf(0);
        //Insert your code here to perform the actual multiplication.
        
        /* Amount of Multiplications */
        for(int i = 0; i < numOfMul; i++){
            /* Grouping */
        	result = (Double.parseDouble(map.get(x + "," + y + "," + i).toString()) * Double.parseDouble(map.get(x + "," + y + "," + (i+numOfMul)).toString()));
            /* Debug Printing */
        	System.out.print("C(" + x + "," + y + ") = " + result + "\t");
            rprtr.incrCounter(ReduceMultiplication.Counters.RESAULT, 1);
            
            //Set these values so the output key represents the correct element of the 
            //result matrix C 
            String rowKey = String.valueOf(x);
            String colKey = String.valueOf(y);
            String elementKey = String.valueOf(i);
            emitKey.set(rowKey + "," + colKey);

            emitValue.set(elementKey + "," + result);
            output.collect(emitKey, emitValue);
        }

    }
}