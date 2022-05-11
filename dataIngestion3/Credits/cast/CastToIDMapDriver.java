import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CastToIDMapDriver {
	public static void main(String... args) throws Exception {
		System.out.println("Started main");
		if (args.length < 2) {
			System.out.println("You are using the wrong format");
			System.err.println("The correct format is : Input path Output path");
			System.exit(-1);
		}
		String inputFile = args[0];
		Path inPath = new Path(inputFile);

		String outputFile = args[1];
		Path outPath = new Path(outputFile);

		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "\t");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		Job job = Job.getInstance(conf);
		job.setJobName("Mapping Cast Name to its ID");
		job.setNumReduceTasks(1);
		job.setJarByClass(CastToIDMapDriver.class);
		job.setMapperClass(CastToIDMapper.class);
		job.setReducerClass(CastToIDReducer.class);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
