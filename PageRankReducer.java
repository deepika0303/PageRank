import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Double pgrank = 0.0;
		String temp = "";
		for (Text value: values){
			String valuestring = value.toString();
			if(valuestring.contains(","))
			{
				String words[] = valuestring.split(",");
				pgrank = pgrank + Double.parseDouble(words[words.length-1]);
			}
			else
			{
				temp = valuestring;
			}
		}
		temp = temp + " " + String.valueOf(pgrank);
		context.write(key, new Text(temp));
	}
}