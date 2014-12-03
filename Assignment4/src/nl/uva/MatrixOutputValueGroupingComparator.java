package nl.uva;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * This class enables us to compare keys like [A,0,0] [A,0,75] and decide if we 
 * have the same matrix. 
 * @author S. Koulouzis
 */
public class MatrixOutputValueGroupingComparator extends WritableComparator {

    public MatrixOutputValueGroupingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        String[] parts1 = wc1.toString().split(",");
        String[] parts2 = wc2.toString().split(",");
        //Check if we have the same matrix
        int returnValue = parts1[0].compareTo(parts2[0]);

        return returnValue;
    }
}