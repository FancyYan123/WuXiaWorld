import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.ansj.domain.Term;
import org.ansj.domain.Result;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

public class GetNames {
    public static class GetNamesMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            //仅将key替换成对应的书名
            Text newKey = new Text();
            newKey.set(((FileSplit) context.getInputSplit()).getPath().getName().split("\\.((TXT)|(txt))\\.segmented")[0]);
            context.write(newKey, value);
        }
    }

    public static class GetNamesReducer extends Reducer<Text, Text, Text, Text>{
        Set<String> allNames = new HashSet<String>();
        static int namesNumThresh = 3;
        static int linesNumThresh = 5;

        @Override
        public void setup(Context context) throws IOException{
            String[] allNamesStr = context.getConfiguration().get("OurNameList").split(" ");
            for(String name: allNamesStr){
                //动态添加词语：
                UserDefineLibrary.insertWord(name, "nr", 1000);
                allNames.add(name);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int countLines = 0;
            Set<String> concurrenceNames = new HashSet<String>();
            for(Text line: values){
                countLines++;
                Result words = ToAnalysis.parse(line.toString());
                for(Term word: words){
                    if(word.getNatureStr().equals("nr")){
                        String name = word.getName();
                        if(allNames.contains(name)){
                            concurrenceNames.add(name);
                        }
                    }
                }
                if(concurrenceNames.size()>=namesNumThresh ||
                        (countLines>=linesNumThresh && concurrenceNames.size()>1)){
                    String namesToWrite = "";
                    for(String each: concurrenceNames){
                        namesToWrite += each+" ";
                    }
                    context.write(key, new Text(namesToWrite));
                    countLines = 0;
                    concurrenceNames.clear();
                }
            }
        }
    }

    public static class FileNameInputFormat extends FileInputFormat<Text, Text>{


        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
            FileNameRecordReader recordReader = new FileNameRecordReader();
            try {
                recordReader.initialize(split, context);
            }catch (Exception e){
                e.printStackTrace();
            }
            return recordReader;
        }
    }

    public static class FileNameRecordReader extends RecordReader<Text, Text>{
        Text fileName;
        LineRecordReader recordReader = new LineRecordReader();

        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException{
            fileName.set(((FileSplit)split).getPath().getName().split("\\.((TXT)|(txt))\\.segmented")[0]);
            recordReader.initialize(split, context);
        }

        public boolean nextKeyValue() throws IOException, InterruptedException{
            return recordReader.nextKeyValue();
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException{
            return fileName;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException{
            return recordReader.getCurrentValue();
        }

        public float getProgress() throws IOException, InterruptedException{
            return recordReader.getProgress();
        }

        public synchronized void close() throws IOException {
            recordReader.close();
        }
    }

    public static void main(String[] args) throws Exception{
        if(args.length != 3){
            System.err.println("Usage: GetNames <Input folder> <Output folder> <NameList>");
            System.err.println("Attention Please: the path of namelist should be in your own file system, not HDFS.");
            System.exit(-1);
        }

        //从文件中获取所有人名
        Scanner fileSC = getFileInput(args[2]);
        String allNames = "";
        while(fileSC.hasNext()){
            allNames += fileSC.next()+" ";
        }
        System.out.println("\n\n\nthe length of allnames is "+allNames.length());


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Names");
        job.setJarByClass(GetNames.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GetNamesMapper.class);
        job.setReducerClass(GetNamesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set("OurNameList", allNames);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Scanner getFileInput(String path){
        FileInputStream inputStream = null;
        Scanner sc = null;
        try{
            inputStream = new FileInputStream(path);
            sc = new Scanner(inputStream, "UTF-8");
            return sc;
        }catch(FileNotFoundException e){
            e.printStackTrace();
            System.out.println("Unknown path!");
        }
        return null;
    }
}
