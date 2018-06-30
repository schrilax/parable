import java.io.DataOutputStream;
import java.io.IOException;
// import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapred.OutputCollector;
// import org.apache.hadoop.mapred.Reporter;
// import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
// import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import weka.clusterers.HierarchicalClusterer;
import weka.core.Attribute;
import weka.core.EuclideanDistance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

/**
 * @author Haimonti
 *
 */
public class CentralizedCentroids {
	public static class IdenMapper extends Mapper<LongWritable, Text, LongWritable, Text> {	
		double rnd = Math.random()*100;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
			LongWritable one = new LongWritable((long) rnd);
			String tempString = value.toString();
			System.out.println("Here is my string "+tempString);
			System.out.println("Rnd val is "+rnd);
			System.out.println("One val is "+one);
			context.write(one, value);
		}
	}

	public static class HierClusReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> lineRead, Context output) throws IOException, InterruptedException {
			double rnd = Math.random()*100;
			
			LongWritable two = new LongWritable((long) rnd);
			System.out.println("Random No. generated is " + two);

			//Provide the attribute names
			FastVector attrbName = new FastVector();

			//Hard-coding attribute names for now
			Attribute attrb1 = new Attribute("preg");
			Attribute attrb2 = new Attribute("plas");
			Attribute attrb3 = new Attribute("pres");
			Attribute attrb4 = new Attribute("skin");
			Attribute attrb5 = new Attribute("insu");
			Attribute attrb6 = new Attribute("mass");
			Attribute attrb7 = new Attribute("pedi");
			Attribute attrb8 = new Attribute("age");
			Attribute clabel = new Attribute("class");
			
			attrbName.addElement(attrb1);
			attrbName.addElement(attrb2);
			attrbName.addElement(attrb3);
			attrbName.addElement(attrb4);
			attrbName.addElement(attrb5);
			attrbName.addElement(attrb6);
			attrbName.addElement(attrb7);
			attrbName.addElement(attrb8);
			attrbName.addElement(clabel);

			//Hard coding the number of attributes including class label
			int numAttrb = 9;

			String instName = "diabetes_" + two;
			Instances actual= new Instances(instName, attrbName,1);
			actual.setClass(clabel);

			for(Text val: lineRead) {
				String tmpln = (val).toString();
				System.out.println("This is what tmpln contains " + tmpln.toString());

				//Convert the line to a double array
				StringTokenizer itr = new StringTokenizer(tmpln, ",");

				//Get all the attributes and the label
				double[] convDb = new double[numAttrb];

				//Get only the attributes without the class label
				int lnLen = 0;
				
				while ((itr.hasMoreTokens()) & (lnLen < numAttrb)) {
					convDb[lnLen] = Double.parseDouble(itr.nextToken());
					lnLen = lnLen + 1;
				}

				//Make the line read an instance
				Instance ist = new Instance(1, convDb);
				
				// Add instance to Instances
				actual.add(ist);
			}

			HierarchicalClusterer HC = new HierarchicalClusterer();
			HC.setNumClusters(2);
			EuclideanDistance dist = new EuclideanDistance();

			HC.setDistanceFunction(dist);
			System.out.println("How many instances ? " + actual.numInstances() );
			System.out.println("Class Label is " + actual.classIndex());

			try {
				HC.buildClusterer(actual);
				System.out.println("Centralized MapReduce Newick format is: "+HC.graph());
				output.write(two, new Text(HC.graph()));
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class MyTextOutputFormat extends FileOutputFormat<LongWritable,Text> {
		@Override
		public org.apache.hadoop.mapreduce.RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
			//get the current path
			Path path = FileOutputFormat.getOutputPath(arg0);
			System.out.println("Output path is " + path);
			
			//create the full path with the output directory plus our filename
			Path fullPath = new Path(path, "result.txt");

			//create the file in the file system
			FileSystem fs = path.getFileSystem(arg0.getConfiguration());
			FSDataOutputStream fileOut = fs.create(fullPath, arg0);

			//create our record writer with the new file
			return new MyCustomRecordWriter(fileOut, arg0.getConfiguration());
		}
	}

	public static class MyCustomRecordWriter extends RecordWriter<LongWritable,Text> {
		private DataOutputStream outStream;
		Configuration cf;

		public MyCustomRecordWriter(DataOutputStream stream, Configuration conf) {	
			outStream = stream;
			this.cf = conf;
			try {
				outStream.writeBytes("results:\r\n");
			}
			catch (Exception ex) {
			}
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			outStream.close();
		}

		@Override
		public void write(LongWritable k1, Text v1) throws IOException, InterruptedException  {
			//write out our key
			outStream.writeBytes(k1.toString() + ": ");
			//loop through all values associated with our key and write them with commas between
			outStream.writeBytes(v1.toString());
			outStream.writeBytes("\r\n");
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CC");
		
		String locDataPath = args[0];
		String outputDir = args[1];
		
		String noOfClusters = args[2];
		conf.set("InputDir", locDataPath);
		conf.set("OutputDir", outputDir);
		conf.set("NumClusters", noOfClusters);

		job.addFileToClassPath(new Path("/jars/weka.jar"));
		
		conf = job.getConfiguration();
		conf.addResource(args[1]);

		job.setJarByClass(CentralizedCentroids.class);	
		job.setMapperClass(IdenMapper.class);
		job.setReducerClass(HierClusReducer.class);

		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(MyTextOutputFormat.class);

		//I am trying to create two mappers the first with three lines
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 2);
		job.setNumReduceTasks(2);
		NLineInputFormat.addInputPath(job, new Path(locDataPath));
		
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
