import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;

import java.io.IOException;
import java.util.ArrayList;


public class LPA {
    public static int MAX_ITERATION_TIME = 25;
    int iteration = 0;
    public static String OUT = "output";
    public static String IN = "input";
    private static Integer keyNum;
    public static class LPASetupMapper extends Mapper<Object, Text, Text, Text> {

        Text key = new Text();
        Text value = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.print("reducess\n");

            String[] strs = value.toString().split("\t");
            String[] people = strs[0].split(",");
            this.key.set(people[0]);
            this.value.set(people[1] + "," + strs[1]);
            context.write(this.key, this.value);
        }
    }


    public static class LPASetupReducer extends Reducer<Text, Text, Text, Text> {
        Text key = new Text();
        Text value = new Text();
        StringBuilder output;//输出的String
        int sum;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            ArrayList<Integer> count = new ArrayList<Integer>();
            ArrayList<String> name = new ArrayList<String>();
            for (Text val : values) {
                String[] res = val.toString().split(",");
                name.add(res[0]);
                int temp = Integer.parseInt(res[1]);
                sum += temp;
                count.add(temp);
            }
            output = new StringBuilder();
            for (int i = 0; i < count.size(); i++) {
                float weight = (float) count.get(i) / sum;
                output.append(name.get(i) + "," + weight + "," + name.get(i) + ";");
            }
            this.value.set(String.format("%s&%s", key.toString(), output.toString()));
            context.write(key, this.value);
        }
    }

    public static class LPAMapper extends Mapper<Object, Text, Text, Text> {
        Text key = new Text();
        Text value = new Text();
        HashMap<String, Double> labelToWeight;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           // System.out.print("mapss\n");
            labelToWeight = new HashMap<String, Double>();
            String[] strs = value.toString().split("\t");
            String[] strTemp = strs[1].split("&");//lable and weight
            String[] peopleAndLable = strTemp[1].split(";");
            ArrayList<String> people = new ArrayList<String>();

            for (String str : peopleAndLable) {
                if (str.length() == 0) {
                    continue;
                }
                String[] list = str.split(",");
                if (!people.contains(list[0]))
                    people.add(list[0]);
                Double weight = Double.valueOf(list[1]);
                Double sum = labelToWeight.get(list[2]);
                if (sum == null) {
                    sum = weight;
                } else {
                    sum += weight;
                }
                labelToWeight.put(list[2], sum);
            }
            double max = 0;
            String lable = null;
            for (String s : labelToWeight.keySet()) {
                double temp = labelToWeight.get(s);
                if (temp > max) {
                    lable = s;
                    max = temp;
                }
            }
            for (String person : people) {
                context.write(new Text(person), new Text(strs[0] + "," + lable));
            }
            this.key.set(strs[0]);
            this.value.set(lable + "&" + strTemp[1]);
            context.write(this.key, this.value);
        }
    }

    public static class LPAReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, String> personToLable = new HashMap<String, String>();
        Text key = new Text();
        Text value = new Text();
        StringBuilder output;
        String person = null;
        String label = null;
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            HashMap<String, String> nameAndLabel = new HashMap<String, String>();
            for (Text text : values) {
                String str = text.toString();
                if (!str.contains("&")) {
                    nameAndLabel.put(str.split(",")[0], str.split(",")[1]);
                } else {
                    person = str.split("&")[1];
                    label = str.split("&")[0];
                }
            }
            output = new StringBuilder();
            output.append(label);
            output.append("&");
            String[] tempList = person.split(";");
            for (String s : tempList) {
                String[] temp = s.split(",");
                String tempS="";
                tempS=tempS+temp[0]+","+temp[1]+","+nameAndLabel.get(temp[0])+";";
                output.append(tempS);
            }
            this.value.set(output.toString());
            context.write(key, value);

        }
    }

    public static class LPACleanupMapper extends Mapper<Object, Text, Text, Text> {
        Text key = new Text();
        Text value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String[] people = strs[1].split("&");
            this.key.set(people[0]);
            this.value.set(strs[0]);
            context.write(this.key, this.value);
        }
    }

    public static class LPACleanupReducer extends Reducer<Text, Text, Text, Text> {
        String oldLabel;
        Integer intLabel;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            oldLabel = null;
            intLabel = 0;
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(oldLabel==null)
                oldLabel = key.toString();
            if(oldLabel != key.toString())
                intLabel++;
            for (Text value : values) {
                context.write(value,new Text(intLabel.toString()+" "+key.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 3){
            System.err.println("Usage: LPA <Input folder>  <Output folder/> <MAX_ITERATION_TIME>");
            System.exit(-1);
        }

        IN = args[0];
        OUT = args[1];
        int MAX = Integer.parseInt(args[2]);
        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;
        Job jobPR = null;
        Configuration conf = new Configuration();

        FileSystem fs = null;

        do {
            System.out.printf("Iteration %d: Start\r\n", iteration);
            jobPR = Job.getInstance(conf, "LPA");
            jobPR.setJarByClass(LPA.class);
            if (iteration == 0) {
                System.out.printf("Iteration %d: setup\r\n", iteration);
                jobPR.setMapperClass(LPA.LPASetupMapper.class);
                jobPR.setReducerClass(LPA.LPASetupReducer.class);
            } else if (iteration == MAX - 1) {
                System.out.printf("Iteration %d: cleanup\r\n", iteration);
                jobPR.setNumReduceTasks(1);
                jobPR.setMapperClass(LPA.LPACleanupMapper.class);
                jobPR.setReducerClass(LPA.LPACleanupReducer.class);
            } else {
                jobPR.setMapperClass(LPA.LPAMapper.class);
                jobPR.setReducerClass(LPA.LPAReducer.class);
            }

            jobPR.setMapOutputKeyClass(Text.class);
            jobPR.setMapOutputValueClass(Text.class);
            jobPR.setOutputKeyClass(Text.class);
            jobPR.setOutputValueClass(Text.class);
            Path inputPath;
            inputPath = new Path(input);
            Path outputPath = new Path(output);
            FileInputFormat.setInputPaths(jobPR, inputPath);
            FileOutputFormat.setOutputPath(jobPR, outputPath);

            if (!jobPR.waitForCompletion(true))
                System.exit(-1);

            // delete the output folder in the previous iteration to save disk space.
            fs = inputPath.getFileSystem(conf);
            if (iteration > 0 && fs.exists(inputPath))
                fs.delete(inputPath, true);


            System.out.printf("Iteration %d: End\r\n", iteration);
            ++iteration;
            input = output;
            output = OUT + iteration;
            if (iteration == MAX - 1){
                output = OUT + "result";
            }
            System.out.printf("Iteration %d: %s %s\r\n", iteration,input,output);

//            jobPR.getCounters().findCounter(PageRank.eInf.COUNTER).setValue(0);
        } while (iteration < MAX);
    }
}
