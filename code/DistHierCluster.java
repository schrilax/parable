import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.clusterers.HierarchicalClusterer;
import weka.core.Attribute;
import weka.core.EuclideanDistance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class DistHierCluster {
	public static class DHCMapper extends Mapper<Object, Text, LongWritable, Text>{
		private LongWritable rand = new LongWritable((long) Math.random()*100);
		private Text word = new Text();

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(values.toString());
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(rand, word);
			}
		}
	}

	public static class DHCReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		private LongWritable rkey = new LongWritable((long) Math.random()*100);
		
		//@Override
	    //protected void setup(Context context) throws IOException, InterruptedException {
		//	clusters = context.getConfiguration().getInt("Clusters", 0);
		//	context.
	    //}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			FastVector attrbName = new FastVector();

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

			String instName = "diabetes";
			Instances actual= new Instances(instName, attrbName, 1);
			actual.setClass(clabel);
			
			int attrib_length = 0;

			for (Text value : values) {
				String[] tokens = value.toString().split(",");
				attrib_length = tokens.length;
				
				double[] attribs = new double[attrib_length];
				
				for (int idx = 0; idx < attrib_length; idx++) {
					attribs[idx] = Double.parseDouble(tokens[idx]);
				}

				Instance ist = new Instance(1, attribs);
				actual.add(ist);
			}
			
			HierarchicalClusterer HC = new HierarchicalClusterer();
			HC.setNumClusters(2);
			
			EuclideanDistance dist = new EuclideanDistance();
			HC.setDistanceFunction(dist);

			try {
				HC.buildClusterer(actual);
				context.write(rkey, new Text(HC.graph()));
				
				for (int idx = 0; idx < actual.numInstances(); idx++) {
					String instance_labels = new String("");
					
					for (int attrib_idx = 0; attrib_idx < attrib_length; attrib_idx++) {
						instance_labels += Double.toString(actual.instance(idx).value(attrib_idx));
						
						if (attrib_idx != (attrib_length - 1)) {
							instance_labels += ",";
						}
					}
					
					instance_labels += ":" + Integer.toString(HC.clusterInstance(actual.instance(idx)));
					context.write(rkey, new Text(instance_labels));
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Distributed Hierarchical Clustering");
		job.addFileToClassPath(new Path("/jars/weka.jar"));

		job.setJarByClass(DistHierCluster.class);

		job.setMapperClass(DHCMapper.class);
		job.setReducerClass(DHCReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
