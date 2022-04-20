import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CreditStats {
	
	public static void main(String... args) throws Exception {
		System.out.println("Started main");
		if (args.length != 2) {
			System.err.println("Usage: CreditStats <Input path> <Output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "\t");
		
		FileSystem fs = FileSystem.get(conf);
		String inFile = args[0];
		String outFile = args[1];
		
		Job job = Job.getInstance(conf);
		job.setJobName("Cast Id-Name mapping");
		job.setJarByClass(CreditStats.class);
		job.setMapperClass(CreditStatsMapperWithCounter.class);
		
		Path inPath = new Path(inFile);
		Path outPath = new Path(outFile);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
