package nl.uva;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author S. Koulouzis
 */
public class AssignmentMapreduce extends Configured implements Tool {

    static Log log = LogFactory.getLog(AssignmentMapreduce.class);
    /**
     * The path in the local file system for matrix A
     */
    private static String matrixA;
    /**
     * The path in the local file system matrix B
     */
    private static String matrixB;
    /**
     * The path in the HDFS for the transposed matrix B
     */
    private static Path transposedMatrixB;
    /**
     * The path in the HDFS for matrix C (the result) with the elements saved in
     * separate lines
     */
    private static Path unformatedMatrixC;
    /**
     * The path in the HDFS for matrix A with row numbers
     */
    private static ArrayList<Path> matrixWithRowNums;
    /**
     * The path in the HDFS for the transposed matrix B
     */
    private static String outputTransposeFolder;
    /**
     * The path in the HDFS for the matrices with row numbers
     */
    private static String outputAddRowNumsFolder;
    /**
     * The path in the HDFS for matrix C (the multiplication result)
     */
    private static String outputMultiplicationFolder;
    /**
     * The path in the HDFS for formated matrix C (the multiplication result)
     */
    private static String outputResultFolder;
    private static int numOfRowsInC;
    private static int numOfColumnsInC;
    private static String outputSumFolder;
    private static Integer minMulPerReducer;
    private static int numOfColumnsInA;
    private static Integer maxMap;
    private Path remotePathMatrixC;

    public static void main(String[] args) {
        try {

            if (args == null || args.length < 3 || args[0].equals("-help") || args[0].equals("help")) {
                printHelp();
                System.exit(-1);
            }
//            args = new String[]{"A", "B", "out", "1"};
            //Set the input arguments 
            matrixA = args[0];
            matrixB = args[1];
            maxMap = Integer.valueOf(args[3]);


            outputTransposeFolder = args[2] + "Transpose";
            outputAddRowNumsFolder = args[2] + "AddRowNums";
            outputMultiplicationFolder = args[2] + "Multiplication";
            outputSumFolder = args[2] + "Sum";
            outputResultFolder = args[2] + "Result";


            //We need to know the number of rows in matrix A and the number of 
            //columns in matrix B. 
            numOfColumnsInA = countColumns(matrixA);
            int numOfRowsInB = countRows(matrixB);

            //If the rows of matrix A don't match those of matrix B we can't do 
            //the multiplication 
            if (numOfColumnsInA != numOfRowsInB) {
                printHelp();
                throw new Exception("Number of columns in matrix A = " + numOfColumnsInA + " don't match the number of rows in matrix B =" + numOfRowsInB);
            }
            numOfRowsInC = countRows(matrixA);
            numOfColumnsInC = countColumns(matrixB);

            String[] myArgs = new String[]{};

            //Start the execution
            int res = ToolRunner.run(new Configuration(), new AssignmentMapreduce(), myArgs);

            System.exit(res);
        } catch (Exception ex) {
            Logger.getLogger(AssignmentMapreduce.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void printHelp() {
        System.out.println("Usage: <matrix A> <matrix B> <outpout folder> <max num. "
                + "of mapred>\n");

        System.out.println("matrix A:\t\t\tThe path for matrix A. It has to be a text "
                + "file separating elements with whitespace. Make sure that the "
                + "beginning of each row doesn't' start with a whitespace.");

        System.out.println("matrix B:\t\t\tThe path for matrix B. It has to be a text "
                + "file separating elements with whitespace. Make sure that the "
                + "beginning of each row doesn't' start with a whitespace.");

        System.out.println("outpout folder:\t\t\tThe location where the results will "
                + "be saved.");

        System.out.println("max num. of mapred:\t\tThe maximum number of map and "
                + "reduce tasks that will be run simultaneously by a task tracker.");

        System.out.println("The result of the multiplication will be saved on a "
                + "text file named C");
    }

    @Override
    /**
     * This method contains the execution logic: 1. Transpose matrix B so we can
     * pass the columns to the reducer reading the file one line at the time 2.
     * Add row numbers to the matrices so we can use them as keys 3. Do the
     * multiplication. The mapper reads both files and passes each line to the
     * reduces which preforms the multiplication 4. Format the result so we have
     * in matrix C line 1: c(1,*) line 2: c(2,*) etc.
     */
    public int run(String[] args) throws Exception {

        minMulPerReducer = (numOfRowsInC * numOfColumnsInC) / maxMap;


        //Transpose matrix B so we can read it line by line
        JobConf jobTrasposeBConf = configureTransposeBJob();
        log.info("Running transpose job");
        JobClient.runJob(jobTrasposeBConf);

        //Rename the result to B. We will need that later so we can identify 
        //which key to emit from the multiplication mapper 
        Path outPutPath = FileOutputFormat.getOutputPath(jobTrasposeBConf);
        FileSystem fs = FileSystem.get(jobTrasposeBConf);
        FileStatus[] files = fs.globStatus(new Path(outPutPath + "/part-*"));
        for (int i = 0; i < files.length; i++) {
            transposedMatrixB = new Path(files[i].getPath().getParent(), "B" + i);
            fs.rename(files[i].getPath(), transposedMatrixB);
        }

        JobConf jobAddRowNumsConf = configureAddRowNumsJob();
        log.info("Running add rown numbers job");
        JobClient.runJob(jobAddRowNumsConf);

        //Rename the results 
        log.info("Renaming matrix A and B");
        outPutPath = FileOutputFormat.getOutputPath(jobAddRowNumsConf);
        fs = FileSystem.get(jobAddRowNumsConf);
        files = fs.globStatus(new Path(outPutPath + "/A*"));
        matrixWithRowNums = new ArrayList<Path>();
        for (int i = 0; i < files.length; i++) {
            Path path = new Path(files[i].getPath().getParent(), "A" + i);
            fs.rename(files[i].getPath(), path);
            matrixWithRowNums.add(path);
        }

        files = fs.globStatus(new Path(outPutPath + "/B*"));
        for (int i = 0; i < files.length; i++) {
            Path path = new Path(files[i].getPath().getParent(), "B" + i);
            fs.rename(files[i].getPath(), path);
            matrixWithRowNums.add(path);
        }

        //Run the actual multiplication 
        JobConf jobMultiplicationConf = configureMultiplicationJob();
        log.info("Running multiplication job");
        JobClient.runJob(jobMultiplicationConf);


        //Rename the result into C 
        outPutPath = FileOutputFormat.getOutputPath(jobMultiplicationConf);
        fs = FileSystem.get(jobMultiplicationConf);
        files = fs.globStatus(new Path(outPutPath + "/part-*"));
        for (int i = 0; i < files.length; i++) {
            unformatedMatrixC = new Path(files[i].getPath().getParent(), "C" + i);
            fs.rename(files[i].getPath(), unformatedMatrixC);
        }


        //Format matrix C (the result of A*B) so each element is not presented 
        //on a separate line 
        JobConf jobSumConf = configureSumJob();
        log.info("Running sum job");
        JobClient.runJob(jobSumConf);


        //Rename and copy the result back to the local file system 
        outPutPath = FileOutputFormat.getOutputPath(jobSumConf);
        fs = FileSystem.get(jobSumConf);
        files = fs.globStatus(new Path(outPutPath + "/part-*"));
        remotePathMatrixC = new Path(files[0].getPath().getParent(), "C");
        fs.rename(files[0].getPath(), remotePathMatrixC);


        //Format matrix C (the result of A*B) so each element is not presented 
        //on a separate line 
        JobConf jobFormatConf = configureFormatJob();
        log.info("Running format job");
        JobClient.runJob(jobFormatConf);

        //Rename and copy the result back to the local file system 
        outPutPath = FileOutputFormat.getOutputPath(jobFormatConf);
        fs = FileSystem.get(jobSumConf);
        files = fs.globStatus(new Path(outPutPath + "/part-*"));
        Path matrixC = new Path(files[0].getPath().getParent(), "C");
        fs.rename(files[0].getPath(), matrixC);

        LocalFileSystem localFileSystem = new LocalFileSystem();
        localFileSystem.initialize(new URI(""), jobSumConf);
        FileUtil.copy(matrixC.getFileSystem(jobSumConf), matrixC, localFileSystem, new Path("C"),
                false, true, jobSumConf);

        return 0;
    }

    /**
     * This method configures the job that transposes matrix B (Turns the
     * columns into rows)
     *
     * @param args
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    private JobConf configureTransposeBJob() throws IOException, URISyntaxException {
        JobConf conf = new JobConf(getConf(), AssignmentMapreduce.class);
        conf.setJobName("MatrixTranspose");

        //Set the Maper and Reducer classes
        conf.setMapperClass(nl.uva.MapTranspose.class);
        conf.setReducerClass(nl.uva.ReduceTranspose.class);

        //Set Input Format and Output Format to apply to the input 
        //files for the mappers 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        //The data types emitted by the reducer are identified by 
        //setOutputKeyClass() and setOutputValueClass()
        //It is the class for the value and output key.
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);


        //Set the output folder 
        FileOutputFormat.setOutputPath(conf, new Path(outputTransposeFolder));

        //Copy matrix B into the DFS 
        LocalFileSystem localFileSystem = new LocalFileSystem();
        localFileSystem.initialize(new URI(""), conf);

        Path localPath = new Path(matrixB);
        Path remoteInputPath = new Path("input/" + localPath.getName());
        if (!FileUtil.copy(localFileSystem, localPath, remoteInputPath.getFileSystem(conf), remoteInputPath,
                false, false, conf)) {
            throw new IOException("Faild to copy input files into HDFS");
        }
        //Set the input path for the job
        FileInputFormat.setInputPaths(conf, remoteInputPath);
        return conf;
    }

    /**
     * This method configures the job that adds row numbers
     *
     * @param args
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    private JobConf configureAddRowNumsJob() throws IOException, URISyntaxException {

        JobConf conf = new JobConf(getConf(), AssignmentMapreduce.class);
        conf.setJobName("addLineNumbers");

        //Set the Maper and Reducer classes
        conf.setMapperClass(nl.uva.MapAddRowNums.class);
        conf.setReducerClass(nl.uva.ReduceAddRowNums.class);
        //Add a custom key and Value Grouping Comparator so we can turn the offset
        //and byte length to line numbers 
        conf.setOutputKeyComparatorClass(MatrixKeyComparator.class);
        conf.setOutputValueGroupingComparator(MatrixOutputValueGroupingComparator.class);

        //Set Input Format and Output Format to apply to the input 
        //files for the mappers 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The data types emitted by the reducer are identified by 
        //setOutputKeyClass() and setOutputValueClass()
        //It is the class for the value and output key.
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);


        //Copy the transposed matrix B
        LocalFileSystem localFileSystem = new LocalFileSystem();
        localFileSystem.initialize(new URI(""), conf);

        int numOfOuts;
        if (maxMap == 1) {
            numOfOuts = 1;
        } else {
            numOfOuts = Math.min((maxMap / 2), numOfColumnsInC);
        }

        //Add Multiple Outputs for the reducer. This way we can do 2 outputs from
        //the reducer
        for (int i = 0; i < numOfOuts; i++) {
            MultipleOutputs.addMultiNamedOutput(conf, "B" + i,
                    TextOutputFormat.class, Text.class, Text.class);
        }
        conf.setInt("number.of.out.B", numOfOuts);


        Path localPath = new Path(matrixA);
        Path remoteInputPathA = new Path("input/" + localPath.getName() + "A");
        if (!FileUtil.copy(localFileSystem, localPath, remoteInputPathA.getFileSystem(conf), remoteInputPathA,
                false, false, conf)) {
            throw new IOException("Faild to copy input files into HDFS");
        }
        //Add Multiple Outputs for the reducer. This way we can do many outputs from
        //the reducer
        if (maxMap == 1) {
            numOfOuts = 1;
        } else {
            numOfOuts = Math.min((maxMap / 2), numOfRowsInC);
        }

        for (int i = 0; i < numOfOuts; i++) {
            MultipleOutputs.addMultiNamedOutput(conf, "A" + i, TextOutputFormat.class,
                    Text.class, Text.class);
        }
        
        //Set the valiables so we can readthem from the mapper/reducer 
        conf.setInt("number.of.out.A", numOfOuts);

        conf.setInt("rows.in.matrixC", numOfRowsInC);
        conf.setInt("columns.in.matrixC", numOfColumnsInC);


        //Set the input path for the job
        FileInputFormat.setInputPaths(conf, remoteInputPathA, transposedMatrixB.getParent());
        //Set the output folder 
        FileOutputFormat.setOutputPath(conf, new Path(outputAddRowNumsFolder));

        return conf;
    }

    /**
     * This method configures the job for the multiplication
     *
     * @return
     */
    private JobConf configureMultiplicationJob() {
        JobConf conf = new JobConf(getConf(), AssignmentMapreduce.class);
        conf.setJobName("MatrixMuliplication");

        //Set the Maper and Combiner classes
        conf.setMapperClass(nl.uva.MapMultiplication.class);
        conf.setReducerClass(nl.uva.ReduceMultiplication.class);


        //Set Input Format and Output Format to apply to the input 
        //files for the mappers 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The data types emitted by the reducer are identified by 
        //setOutputKeyClass() and setOutputValueClass()
        //Define the class for the output key.
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Set the input and output folders
        FileOutputFormat.setOutputPath(conf, new Path(outputMultiplicationFolder));
        Path[] arrayMatrixWithRowNums = new Path[matrixWithRowNums.size()];
        arrayMatrixWithRowNums = matrixWithRowNums.toArray(arrayMatrixWithRowNums);
        FileInputFormat.setInputPaths(conf, arrayMatrixWithRowNums);
        
        //Set the number of rows in A and columns in B
        conf.setInt("rows.in.matrixC", numOfRowsInC);
        conf.setInt("columns.in.matrixC", numOfColumnsInC);
        conf.setInt("columns.in.matrixA", numOfColumnsInA);
        conf.setInt("multiplications.per.reducer", minMulPerReducer);
        conf.setNumReduceTasks(maxMap);
        conf.setNumMapTasks(maxMap);
        return conf;
    }

    /**
     * This method configures the job for formating the result
     *
     * @return
     */
    private JobConf configureSumJob() {
        JobConf conf = new JobConf(getConf(), AssignmentMapreduce.class);
        conf.setJobName("MatrixSum");


        //Set the Maper and Combiner classes
        conf.setMapperClass(nl.uva.MapSum.class);
        conf.setReducerClass(nl.uva.ReduceSum.class);

        //Set Input Format and Output Format to apply to the input 
        //files for the mappers 
//        LineInputFormat.setNumLinesPerSplit(conf, linesPerSplit);
//        conf.setInputFormat(LineInputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The data types emitted by the reducer are identified by 
        //setOutputKeyClass() and setOutputValueClass()
        //Define the class for the output key.
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Set the input and output folder 
        FileOutputFormat.setOutputPath(conf, new Path(outputSumFolder));
        FileInputFormat.setInputPaths(conf, unformatedMatrixC.getParent());

//        conf.setNumReduceTasks(maxMap);
        return conf;
    }

    private JobConf configureFormatJob() {
        JobConf conf = new JobConf(getConf(), AssignmentMapreduce.class);
        conf.setJobName("MatrixFormat");

        //Set the Maper and Combiner classes
        conf.setMapperClass(nl.uva.MapFormat.class);
        conf.setReducerClass(nl.uva.ReduceFormat.class);

        //Set Input Format and Output Format to apply to the input 
        //files for the mappers 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The data types emitted by the reducer are identified by 
        //setOutputKeyClass() and setOutputValueClass()
        //Define the class for the output key.
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);


        FileOutputFormat.setOutputPath(conf, new Path(outputResultFolder));
        FileInputFormat.setInputPaths(conf, remotePathMatrixC);

        conf.setNumMapTasks(maxMap);
        return conf;
    }

    private static int countColumns(String matrixPath) throws FileNotFoundException, IOException {

        int columns;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(matrixPath));
            columns = 0;
            String line = reader.readLine();
            columns = line.split(" ").length;
        } finally {
            reader.close();
        }
        return columns;
    }

    private static int countRows(String matrixPath) throws FileNotFoundException, IOException {
        int rows;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(matrixPath));
            rows = 0;
            while (reader.readLine() != null) {
                rows++;
            }
        } finally {
            reader.close();
        }

        return rows;
    }
}