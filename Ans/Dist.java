import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dist {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] itr = value.toString().toLowerCase().split("[\\p{Punct}\\s]+");
            String[] paths = ((FileSplit) context.getInputSplit()).getPath().toString().split("/");
            Text fileName = new Text(paths[paths.length - 1]);
            for (String s:itr){
                word.set(s.trim());
                if (word.toString().startsWith("ex")) {
                    context.write(fileName, word);

                }
            }
        }
    }

    public static class DistReducer
            extends Reducer<Text, Text, Text, Text> {
        private SortedMap<Text, Integer> totalWordsMap = new TreeMap<>();

        public String getCorrectOutputFormat( List<Map.Entry<Text, Integer>> entries){
            String outputFormat = new String(" ");
            for(int i =0;i<entries.size();i++){
                outputFormat+= (entries.get(i).getKey().toString() + ", " + entries.get(i).getValue());
                if(i!=entries.size()-1){
                    outputFormat+=", ";
                }
            }
            return outputFormat;
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            SortedMap<Text, Integer> wordsMap = new TreeMap<>();


            for (Text val : values) {
                if (wordsMap.containsKey(val)) {
                    wordsMap.put(new Text(val), wordsMap.get(val) + 1);
                } else {
                    wordsMap.put(new Text(val), 1);
                }

                if (totalWordsMap.containsKey(val)) {
                    totalWordsMap.put(new Text(val), totalWordsMap.get(val) + 1);
                } else {
                    totalWordsMap.put(new Text(val), 1);
                }
            }
            List<Map.Entry<Text, Integer>> entries = new ArrayList<> (wordsMap.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<Text, Integer>>() {
                @Override
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                   if(o1.getValue().equals(o2.getValue())){
                       return o1.getKey().compareTo(o2.getKey());
                   }
                   else{
                       return Integer.compare(o2.getValue(), o1.getValue());
                   }
                }

            });


            context.write(key, new Text(getCorrectOutputFormat(entries)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Text, Integer>> entries = new ArrayList<> (totalWordsMap.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<Text, Integer>>() {
                @Override
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    if(o1.getValue().equals(o2.getValue())){
                        return o1.getKey().compareTo(o2.getKey());
                    }
                    else{
                        return Integer.compare(o2.getValue(), o1.getValue());
                    }
                }

            });

            context.write(new Text("Total"), new Text(getCorrectOutputFormat(entries)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Dist.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DistReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}