import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;


import java.io.IOException;


public class Normalization {
    public static class NormalizationMapper extends Mapper<Object, Text, Text, Text> {
        Text key = new Text();
        Text value = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []strs = value.toString().split("\t");
            String []people = strs[0].split(",");
            this.key.set(people[0]);
            this.value.set(people[1]+","+strs[1]);
            context.write(this.key, this.value);
        }
    }


    public static class NormalizationReducer extends Reducer<Text, Text, Text, Text> {

        StringBuilder output ;//输出的String
        int sum;
        float PageRankInitialValue = 1;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sum=0;
            ArrayList<Integer>count = new ArrayList<Integer>();
            ArrayList<String>name = new ArrayList<String>();
            for (Text val:values){
                String[] res = val.toString().split(",");
                name.add(res[0]);
                int temp = Integer.parseInt(res[1]);
                sum += temp;
                count.add(temp);
            }
            output = new StringBuilder();
            for (int i=0;i<count.size();i++){
                float weight = (float)count.get(i)/sum;
                output.append(name.get(i)+" "+weight+" ");
            }
            output.delete(output.length(),output.length());
            Text mapValue = new Text(String.format("1.0|%s", output.toString()));
            context.write(key,mapValue);
        }
    }

    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.err.println("Usage: Normalization <Input folder>  <Output folder>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Normalization");
        job.setJarByClass(Normalization.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Normalization.NormalizationMapper.class);
        job.setReducerClass(Normalization.NormalizationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
