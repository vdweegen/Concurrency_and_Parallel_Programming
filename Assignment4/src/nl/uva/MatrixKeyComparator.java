package nl.uva;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * This class is used to compare keys emitted from MapAddRowNums class so they 
 * can be grouped into the same ReduceAddRowNums. For example the MapAddRowNums
 * emits the keys [A,0,75],[A,0,150] and so. With this class we make sure that 
 * all three values are equal
 * 
 * @author S. Koulouzis
 */
public class MatrixKeyComparator extends WritableComparator {

    public MatrixKeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        String[] wcTokens1 = wc1.toString().split(",");
        String[] wcTokens2 = wc2.toString().split(",");

        Long offset1 = new Long(wcTokens1[1]);
        Long offset2 = new Long(wcTokens2[1]);
        Long possision1 = new Long(wcTokens1[2]);
        Long possision2 = new Long(wcTokens2[2]);

        int match = wcTokens1[0].compareTo(wcTokens2[0]);
        if (match == 0) {
            match = offset1.compareTo(offset2);
            if (match == 0) {
                match = possision1.compareTo(possision2);
            }
        }

        return match;
    }
}
