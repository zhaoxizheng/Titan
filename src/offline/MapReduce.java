package offline;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TimeCount {
     public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
         private final static IntWritable one = new IntWritable(1);
         private Text word = new Text();

         public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
             String line = value.toString();
             List<String> values = Arrays.asList(line.split(" "));

             String ip = values.size() > 0 ? values.get(0) : null;
             String timestamp = values.size() > 3 ? values.get(3) : null;
             String method = values.size() > 5 ? values.get(5) : null;
             String url = values.size() > 6 ? values.get(6) : null;
             String status = values.size() > 8 ? values.get(8) : null;

             Pattern pattern = Pattern.compile("\\[(.+?):(.+)");
             Matcher matcher = pattern.matcher(timestamp);
             matcher.find();

             output.collect(new Text(matcher.group(2)).substring(0, 5), 1);
         }
     }

     public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
             int sum = 0;
             while (values.hasNext()) {
                 sum += values.next().get();
             }
             output.collect(key, new IntWritable(sum));
         }
     }

     public static void main(String[] args) throws Exception {
         JobConf conf = new JobConf(TimeCount.class);
         conf.setJobName("timecount");

         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(IntWritable.class);

         conf.setMapperClass(Map.class);
         conf.setCombinerClass(Reduce.class);
         conf.setReducerClass(Reduce.class);

         conf.setInputFormat(TextInputFormat.class);
         conf.setOutputFormat(TextOutputFormat.class);

         FileInputFormat.setInputPaths(conf, new Path(args[0]));
         FileOutputFormat.setOutputPath(conf, new Path(args[1]));

         JobClient.runJob(conf);
     }
}