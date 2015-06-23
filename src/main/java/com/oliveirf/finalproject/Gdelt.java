package com.oliveirf.finalproject;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
public class Gdelt {

    public static class GdeltMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text countryCode = new Text();
        private double num_items;
        private Text date = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split("\t");
            countryCode.set(valor[44]);
            date.set(valor[1]);
	    if(countryCode != null){
		    context.write(countryCode, date);
	    }
	}
    }

    public static class CustomReducer extends Reducer<Text, Text, Text, Text> {
	    private static Hashtable<String, Integer> myhash = new Hashtable<String, Integer>();
	    MultipleOutputs<Text, Text> mos;
	    @Override
		    public void setup(Context context) {
			    mos = new MultipleOutputs(context);
		    }
	    @Override
		    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
			   InterruptedException {
				   for (Text val : values) {
					   Integer occurencies = myhash.get(val.toString());
					   if (occurencies == null) {
						   myhash.put(val.toString(), 1);
					   } else {
						   myhash.put(val.toString(), occurencies + 1);
					   }
				   }
				   for (String mkey : myhash.keySet()) {
					   //System.err.println("key"+key+"-----Key:------"+mkey +"---"+ myhash.get(mkey));
					   if (key.toString().equals("IN") || key.toString().equals("US") ){
						   mos.write(key.toString(),new Text(mkey),new Text(""+myhash.get(mkey)));
					   }
				   }
			   }

	    @Override
		    protected void cleanup(Context context) throws IOException, InterruptedException {
			    mos.close();
		    }
    }

    public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	    Path wikistats = new Path(otherArgs[0]);
	    Path join_result = new Path(otherArgs[1]);

	    Job job = Job.getInstance(conf);
	    job.setJarByClass(Gdelt.class);
	    job.setJobName("FinalProject");

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setMapperClass(GdeltMapper.class);
	    FileInputFormat.addInputPath(job, wikistats);
	    job.setReducerClass(CustomReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    TextOutputFormat.setOutputPath(job, join_result);
	    MultipleOutputs.addNamedOutput(job, "IN", TextOutputFormat.class, Text.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "US", TextOutputFormat.class, Text.class, Text.class);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

