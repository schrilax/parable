import java.io.IOException;
import java.util.Enumeration;
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

import weka.clusterers.EM;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class DHC {
	public static final String FLAGS_NUM_CLUSTERS = "dhc.clusters";
	public static final String FLAGS_NUM_ITERATIONS = "dhc.em.iterations";
	public static final String FLAGS_NUM_INSTANCES = "dhc.em.instances";
	
	public static final int instances_count = 150;
	public static final int clusters_count = 3;
	public static final int iterations_count = 100;
	public static final int max_index = 1000;
	
	public static class DHCM extends Mapper<Object, Text, LongWritable, Text>{
		private EM clusterer = new EM();
		private String options[] = {"-N", Integer.toString(clusters_count), "-I", Integer.toString(iterations_count),};
		
		private String word = new String("");
		
		private FastVector attrbName = new FastVector();
		private Instances data = new Instances("", attrbName, 1);

		private Attribute attrb1 = new Attribute("sepl");
		private Attribute attrb2 = new Attribute("sepw");
		private Attribute attrb3 = new Attribute("petl");
		private Attribute attrb4 = new Attribute("petw");
		
		private Integer labels[] = new Integer[instances_count];
		private LongWritable rkey = new LongWritable((long) (System.currentTimeMillis() % max_index));
		private int idx = 0;
		
		@Override
		protected void setup(Mapper<Object, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			
			for (idx = 0; idx < instances_count; idx++) {
				labels[idx] = 0;
			}
			
			attrbName.addElement(attrb1);
			attrbName.addElement(attrb2);
			attrbName.addElement(attrb3);
			attrbName.addElement(attrb4);
		}

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(values.toString());
			
			while (itr.hasMoreTokens()) {
				String[] tokens = itr.nextToken().split(",");
				double[] vals = new double[tokens.length];
				
				for (idx = 0; idx < tokens.length; idx++) {
					vals[idx] = Double.parseDouble(tokens[idx]);
				}

				Instance ist = new Instance(1, vals);
				data.add(ist);
			}
			
			try {
				clusterer.setOptions(options);
				clusterer.buildClusterer(data);
				
				idx = 0;
				Enumeration<?> e = data.enumerateInstances();
				
				while (e.hasMoreElements()) {
					labels[idx] = clusterer.clusterInstance((Instance) e.nextElement());
					idx++;
				}
				
				for (idx = 0; idx < data.numInstances(); idx++) {
					word = Integer.toString(idx) + ":" + Integer.toString(labels[idx]);
					context.write(rkey, new Text(word));
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class DHCR extends Reducer<LongWritable, Text, LongWritable, Text> {
		private String instance_labels = new String("");
		private Integer labels[] = new Integer[instances_count];
		
		@Override
		protected void setup(Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			
			for (int idx = 0; idx < instances_count; idx++) {
				labels[idx] = 0;
			}
		}

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String[] tokens = value.toString().split(":");
				int instance_id = Integer.parseInt(tokens[0]);
				int instance_label = Integer.parseInt(tokens[1]);
				
				labels[instance_id] = instance_label;
			}
			
			for (int idx = 0; idx < instances_count; idx++) {
				instance_labels = Integer.toString(idx+1) + ":" + Integer.toString(labels[idx]);
				context.write(key, new Text(instance_labels));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "DHC");
		job.addFileToClassPath(new Path("/jars/weka.jar"));
		
		job.getConfiguration().set(FLAGS_NUM_CLUSTERS, Integer.toString(clusters_count));
		job.getConfiguration().set(FLAGS_NUM_ITERATIONS, Integer.toString(iterations_count));
		job.getConfiguration().set(FLAGS_NUM_INSTANCES, Integer.toString(instances_count));
		job.setJarByClass(DHC.class);

		job.setMapperClass(DHCM.class);
		job.setReducerClass(DHCR.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", instances_count);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

/*public class DHC {
	public static class DHCM extends Mapper<Object, Text, LongWritable, Text>{
		private int MAX = 1000;
		
		private LongWritable rand = new LongWritable((long) (System.currentTimeMillis() % MAX));
		private Text word = new Text();

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(values.toString());
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(rand, word);
			}
		}
	}
	
	public static class DHCR extends Reducer<LongWritable, Text, LongWritable, Text> {
		//private LongWritable rkey = new LongWritable((long) Math.random()*100);
		private List<Integer> labels = new ArrayList<Integer>();
		
		private EM clusterer = new EM();
		private String options[] = {"-N", Integer.toString(3), "-I", Integer.toString(100),};
		private String instance_labels = new String("");
		
		private FastVector attrbName = new FastVector();
		private Instances data = new Instances("", attrbName, 1);

		private Attribute attrb1 = new Attribute("sepl");
		private Attribute attrb2 = new Attribute("sepw");
		private Attribute attrb3 = new Attribute("petl");
		private Attribute attrb4 = new Attribute("petw");

		@Override
		protected void setup(Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			
			attrbName.addElement(attrb1);
			attrbName.addElement(attrb2);
			attrbName.addElement(attrb3);
			attrbName.addElement(attrb4);
		}

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String[] tokens = value.toString().split(",");
				double[] vals = new double[tokens.length];
				
				for (int idx = 0; idx < tokens.length; idx++) {
					vals[idx] = Double.parseDouble(tokens[idx]);
				}

				Instance ist = new Instance(1, vals);
				data.add(ist);
			}
			
			try {
				clusterer.setOptions(options);
				clusterer.buildClusterer(data);

				Enumeration<?> e = data.enumerateInstances();
				
				while (e.hasMoreElements()) {
					labels.add(clusterer.clusterInstance((Instance) e
							.nextElement()));
				}
				
				for (int idx = 0; idx < data.numInstances(); idx++) {
					instance_labels += Integer.toString(idx+1) + ":" + Integer.toString(labels.get(idx)) + ", ";
					context.write(key, new Text(instance_labels));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "DHC");
		job.addFileToClassPath(new Path("/jars/weka.jar"));

		job.setJarByClass(DHC.class);

		job.setMapperClass(DHCM.class);
		job.setReducerClass(DHCR.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50);
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}*/
