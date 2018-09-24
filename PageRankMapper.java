import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String temp = "";
		String words[] = line.split(" ");
		Double prval = Double.parseDouble(words[words.length-1])/(words.length-2);
		String outbound = "";
		for (int i=1; i<=words.length-2;i++ ) {
			
			outbound = outbound + words[i]+ " ";
			temp = words[0] + "," + String.valueOf(prval);
			context.write(new Text(words[i]), new Text(temp));
		}
		context.write(new Text(words[0]), new Text(outbound.trim()));
	}
}