import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCount {

  public static class WCMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class WCReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

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

      double rnd = Math.random()*100;

      LongWritable two = new LongWritable((long) rnd);
      System.out.println("Random No. generated is " + two);

      String instName = "diabetes_" + two;
      Instances actual= new Instances(instName, attrbName,1);
      actual.setClass(clabel);
      
      int numAttrb = 9;
      double[] convDb = new double[numAttrb];
      Instance ist = new Instance(1, convDb);
      System.out.println(ist.toString());
      
      HierarchicalClusterer HC = new HierarchicalClusterer();
      HC.setNumClusters(2);
      EuclideanDistance dist = new EuclideanDistance();

      HC.setDistanceFunction(dist);

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "Word Count");
    job.addFileToClassPath(new Path("/jars/weka.jar"));
    
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WCMapper.class);
    job.setCombinerClass(WCReducer.class);
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}