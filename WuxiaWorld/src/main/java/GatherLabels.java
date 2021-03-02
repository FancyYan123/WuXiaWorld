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

public class GatherLabels {
    public static class GatherLabelsMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            String[] NameLabel = value.toString().split("\t");

            Text newKey = new Text(NameLabel[1]);
            Text newValue = new Text(NameLabel[0]);

            context.write(newKey, newValue);
        }
    }



    public static class GatherLabelsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            for(Text each:values) {
                context.write(each, key);
            }
        }
    }

    static public void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: GatherLabels <Input folder> <Output folder>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "GatherLabels");
        job.setJarByClass(SortPR.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GatherLabelsMapper.class);
        job.setReducerClass(GatherLabelsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
