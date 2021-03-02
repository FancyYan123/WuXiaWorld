import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class PageRank {
    public static boolean DEBUG = false;
    public static String OUT = "output";
    public static String IN = "input";
    public static double DAMPING = 0.8;
    public static double INITIAL_VALUE = 1.0;
    public static int MAX_ITERATION_TIME = 1000;

    public enum eInf{
        COUNTER
    }

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text>{
        private Text key = new Text();
        private Text value = new Text();

        //  A PageRankInitialValue | B0 weight0 B1 weight1
        @Override
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            String[] info = value.toString().split("\\|");
            if(info.length == 1){
                String[] tmp = new String[2];
                tmp[0] = info[0];
                tmp[1] = "";
                info = tmp;
            }
            info[1] = info[1].trim();
            String[] nodeInfo = info[0].trim().split("[\\t\\n\\s]+");
            Double pageRankValue = Double.parseDouble(nodeInfo[1]);

            // wirte A B0 weight0 B1 weight1
            this.key.set(nodeInfo[0]);
            this.value.set(info[1]);
            context.write(this.key, this.value);

            // write A PageRankValue
            this.value.set("" + (-pageRankValue));
            context.write(this.key, this.value);

            StringTokenizer outs = new StringTokenizer(info[1]);
            while(outs.hasMoreTokens()){
                String target = outs.nextToken();
                Double weight = Double.parseDouble(outs.nextToken()) * pageRankValue;
                this.key.set(target);
                this.value.set(weight.toString());
                context.write(this.key, this.value);
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private double damping;
        private double initialValue;
        private Text value = new Text();
        private static final String VALUE_FORMAT = "%f|%s";

        public static boolean isDouble(String s){
            try{
                Double.parseDouble(s);
                return true;
            } catch (NumberFormatException e){
                return false;
            }
        }

        @Override
        public void setup(Context context){
            this.damping = context.getConfiguration().getDouble("damping", 1.0);
            this.initialValue = context.getConfiguration().getDouble("initialValue", 0.0);
        }

        //  A PageRankInitialValue|B0 weight0 B1 weight1
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
            double pageRankValue = (1.0 - this.damping) * initialValue;
            double lastRankValue = 0.0;
            double itrRankValue = 0.0;
            String info = "";
            for(Text each : values){
                String tmp = each.toString();
                if(PageRank.PageRankReducer.isDouble(tmp)){
                    double value = Double.parseDouble(tmp);
                    if(value < 0){
                        // Trick: last rank value is negative;
                        lastRankValue = -value;
                    }
                    else{
                        itrRankValue += value;
                    }
                }
                else{
                    info = tmp;
                }
            }

            pageRankValue += this.damping * itrRankValue;
            String value = String.format(PageRankReducer.VALUE_FORMAT, pageRankValue, info);
            this.value.set(value);
            context.write(key, this.value);
            if(Math.abs(pageRankValue - lastRankValue) > 1e-6){
                context.getCounter(eInf.COUNTER).increment(1L);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PageRank <Input folder> <Output folder> <Sorted Output folder>");
            System.exit(-1);
        }

        IN = args[0];
        OUT = args[1];
        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;
        Job jobPR = null;
        Configuration conf = new Configuration();
        conf.setDouble("damping", PageRank.DAMPING);
        conf.setDouble("initialValue", PageRank.INITIAL_VALUE);
        FileSystem fs = null;

        do{
            System.out.printf("Iteration %d: Start\r\n", iteration);
            jobPR = Job.getInstance(conf, "PageRank");
            jobPR.setJarByClass(PageRank.class);
            jobPR.setMapperClass(PageRank.PageRankMapper.class);
            jobPR.setReducerClass(PageRank.PageRankReducer.class);
            jobPR.setMapOutputKeyClass(Text.class);
            jobPR.setMapOutputValueClass(Text.class);
            jobPR.setOutputKeyClass(Text.class);
            jobPR.setOutputValueClass(Text.class);
            Path inputPath;
            if(DEBUG)
                inputPath = new Path(input+"/part-r-00000");
            else
                inputPath = new Path(input);
            Path outputPath = new Path(output);
            FileInputFormat.setInputPaths(jobPR, inputPath);
            FileOutputFormat.setOutputPath(jobPR, outputPath);

            if(!jobPR.waitForCompletion(true))
                System.exit(-1);

            // delete the output folder in the previous iteration to save disk space.
            fs = inputPath.getFileSystem(conf);
            if(iteration != 0 && fs.exists(inputPath))
                fs.delete(inputPath, true);

            if(jobPR.getCounters().findCounter(PageRank.eInf.COUNTER).getValue() == 0)
                break;

            System.out.printf("Iteration %d: End\r\n", iteration);
            ++iteration;
            input = output;
            output = OUT + iteration;
//            jobPR.getCounters().findCounter(PageRank.eInf.COUNTER).setValue(0);
        }while(iteration < PageRank.MAX_ITERATION_TIME);

        System.out.printf("Iteration Times: %d\r\n", iteration);
        String[] sortPRArgs = new String[]{output, args[2]};

        SortPR.main(sortPRArgs);
    }
}
