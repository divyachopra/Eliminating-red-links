import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class WikiLinksMapperOne extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			
			try {
			DocumentBuilderFactory docFactory =  DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document doc =docBuilder.parse(new ByteArrayInputStream(("<xmlns>"+value+"</xmlns>").getBytes()));
			NodeList list = doc.getElementsByTagName("page");
			for(int len=0; len<list.getLength(); len++)
			{  
				Node node = list.item(len);
				 if (node.getNodeType() == Node.ELEMENT_NODE){
				Element element = (Element)node;
				String pageTitle = element.getElementsByTagName("title").item(0).getTextContent().trim().replaceAll(" ", "_").toLowerCase();
				//System.out.println("Page Title : "+pageTitle);
				String pageLinks = element.getElementsByTagName("text").item(0).getTextContent();
				//System.out.println("Page Links :" +pageLinks);
			 	Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
              	Matcher matcher = pattern.matcher(pageLinks);
              	output.collect(new Text(pageTitle), new Text("#"));
              	Boolean bool = false;
              	while(matcher.find())
              	{
              		bool = true;
              		String outLink = matcher.group(1).trim().replaceAll(" ", "_").toLowerCase();
              		//System.out.println("here-------------->"+outLink);
              		output.collect((new Text(outLink)), new Text(pageTitle));
              		
              		
              	}
              	if(!bool)
          		{
          			output.collect(new Text(pageTitle), new Text("null"));
          		}
				 }
				
			}
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
	}