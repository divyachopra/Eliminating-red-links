import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WikiLinksReducerTwo extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter report)
			throws IOException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		while(value.hasNext())
		{
			sb.append(value.next().toString()+ "\t");
		}
		if(sb.length()>0)
			sb.deleteCharAt(sb.length()-1);
			
		output.collect(key, new Text(sb.toString()));
		
	}

}
