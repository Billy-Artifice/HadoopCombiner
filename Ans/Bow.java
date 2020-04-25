import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

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

public class Bow {
    private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
            "it", "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they",
            "we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so",
            "up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no",
            "just", "him", "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see",
            "other", "than", "then", "now", "look", "only", "come", "its", "over", "think", "also", "back", "after",
            "use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want", "because", "any",
            "these", "give", "day", "most", "us" };

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private final static ArrayList<String> top100WordArrayList = new ArrayList<>(Arrays.asList(top100Word));
        private Text word = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] itr = value.toString().toLowerCase().split("[\\p{Punct}\\s]+");
            String[] paths = ((FileSplit) context.getInputSplit()).getPath().toString().split("/");
            Text fileName = new Text(paths[paths.length-1]);
            for (String s:itr){
                word.set(s.trim());
                if(top100WordArrayList.contains(word.toString())){
                    context.write(fileName, word);

                }
            }

        }
    }

    public static class BowReducer
            extends Reducer<Text, Text, Text, Text> {

        public String getCorrectOutputFormat(int[] commonWordsVector){
            String outputFormat = new String("");
            for(int i =0;i<commonWordsVector.length;i++){
                outputFormat+=commonWordsVector[i];
                if(i!=commonWordsVector.length-1){
                    outputFormat+=", ";
                }
            }
            return outputFormat;
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int[] mostCommonWordVector = new int[top100Word.length];
            for (Text val : values) {
                for(int i = 0; i<top100Word.length;i++){
                    if(top100Word[i].equals(val.toString())){
                        mostCommonWordVector[i] = mostCommonWordVector[i]+1;
                        break;
                    }
                }

            }
            context.write(key, new Text(getCorrectOutputFormat(mostCommonWordVector)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Bow.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(BowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}