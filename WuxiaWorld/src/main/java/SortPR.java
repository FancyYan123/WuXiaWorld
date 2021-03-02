import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class SortPR {
    public static class SortPRMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            String[] KeyValue = value.toString().split("\t");

            String[] originValues = KeyValue[1].split("\\|");

            //这里对原有值取负，使得按照从大到小的顺序排列
            DoubleWritable newKey = new DoubleWritable(-Double.parseDouble(originValues[0]));
            Text newValue = new Text(KeyValue[0]+"|"+originValues[1]);
            context.write(newKey, newValue);
        }

    }



    public static class SortPRReducer extends Reducer<DoubleWritable, Text, Text, Text> {

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            key.set(-key.get());
            for(Text each:values) {
                String[] originValues = each.toString().split("\\|");
                Text newKey = new Text(originValues[0]);
                Text newValue = new Text(key.toString() + "|" + originValues[1]);
                context.write(newKey, newValue);
            }
        }
    }

    static public void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: SortPR <Input folder> <Output folder>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SortPR");
        job.setJarByClass(SortPR.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(SortPRMapper.class);
        job.setReducerClass(SortPRReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
