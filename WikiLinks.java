import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class  WikiLinks {

    public static long count = 0;

    public static void main(String[] args) throws Exception {
        WikiLinks mainObject = new WikiLinks();

        String input=args[0];
        String output=args[1];

        int noOfIterations = 8;
        String loc = output + "/graph/";
        String folder = output + "/tmp/";

        //Predefined locations for results output
        String WikiLinkOutput="WikiLinks.out";
        String WikiLinkOutputStage1="WikiLinks.stage1.out";
        String LinkCounterOuput="WikiLinks.n.out";

        String Out1Sorted = "WikiLinks.out1.sorted.out";
        String Out8Sorted = "WikiLinks.out8.sorted.out";

        String[] iterations = new String[noOfIterations+1];

        for ( int i =0; i <= noOfIterations ; i++){
            iterations[i] = "WikiLinks.iter" + Integer.toString(i) +".out";
        }

        //Call to the in-link generation Hadoop task
        mainObject.OutlinkGenrationJob1(input, folder + WikiLinkOutputStage1);
        //System.out.println("After 1");
        mainObject.OutlinkGenrationJob2(folder + WikiLinkOutputStage1, loc );
        //System.out.println("After 2");
        

    }

    public void OutlinkGenrationJob1(String input, String output) throws IOException {
        
    	JobConf conf = new JobConf(WikiLinks.class);
        conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
        conf.set(XMLInputFormat.END_TAG_KEY, "</page>");
        conf.setJarByClass(WikiLinks.class);

        //Configure the inlink generation mapper
        
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XMLInputFormat.class);
        conf.setMapperClass(WikiLinksMapperOne.class);

        //Configure the inlink genration reducer
        
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(WikiLinksReducerOne.class);

        //Define the output key and value classes
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }

    public void OutlinkGenrationJob2(String input, String output) throws IOException {
       
    	JobConf conf = new JobConf(WikiLinks.class);
        conf.setJarByClass(WikiLinks.class);

        //Configure the inlink generation mapper
        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(WikiLinksMapperTwo.class);

        //Configure the inlink genration reducer
        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(WikiLinksReducerTwo.class);

        //Defince the output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //Start the hadoop job
        JobClient.runJob(conf);
    }


  
   }
