import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WikiLinksReducerOne extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{

		@Override
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			HashSet<String> set = new HashSet<String>();
			while(value.hasNext())
			{    //System.out.println("red--------------->");
				set.add(value.next().toString());
			}
			 if (set.contains("#")){
	            	set.remove("#");
			 
			 Iterator i = set.iterator();
         	boolean bool=true;
             while ( i.hasNext()){
             	bool=false;
                 String link = (String) i.next();
                 if ( !link.equals("NULL")){
                	 //System.out.println("After red1");
                     output.collect(new Text(link), key);
                     //System.out.println("here");
                 }
                 else{
                 	output.collect(key,new Text("NULL"));
                 	//System.out.println("here44");
                 }
             }
			 }
			
		}
		
	}