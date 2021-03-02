import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class WordConcurrence {

    public static class WordConcurrenceMapper extends Mapper<Object, Text, Text, LongWritable> {
        private Text pair = new Text();
        private final LongWritable count = new LongWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] names = value.toString().split("[\\n\\t\\s]+");
            for(int i=1; i<names.length; ++i){
                for(int j=1; j<names.length; ++j){
                    if(i == j)
                        continue;
                    pair.set(names[i] + "," + names[j]);
                    context.write(pair, count);
                }
            }
        }
    }

    public static class WordConcurrenceReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable count = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException{
            long count = 0;
            for(LongWritable value : values){
                count += value.get();
            }
            this.count.set(count);
            context.write(key, this.count);
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: WordConcurrence <Input folder> <Output folder>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordConcurrence");


        job.setJarByClass(WordConcurrence.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordConcurrence.WordConcurrenceMapper.class);
        job.setCombinerClass(WordConcurrence.WordConcurrenceReducer.class);
        job.setReducerClass(WordConcurrence.WordConcurrenceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
