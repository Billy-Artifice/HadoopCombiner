import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperCombinerWordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private java.util.Map<String,Integer> wordMap = new HashMap<String,Integer>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] words = value.toString().toLowerCase().split("[\\p{Punct}\\s]+");

            for (String s:words){
                if(!s.trim().equals("")){
                    if(wordMap.containsKey(s)){
                        int sum = (int) wordMap.get(s) + 1;
                        wordMap.put(s, sum);
                    }
                    else {
                        wordMap.put(s, 1);
                    }
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for(String key:wordMap.keySet()){
                Integer value = wordMap.get(key);
                context.write(new Text(key), new IntWritable(value));
            }

        }
    }



    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Date startDate = new Date();
        long startTime = startDate.getTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(InMapperCombinerWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        Date endDate = new Date();
        long endTime = endDate.getTime();

        System.out.println("Total running time of traditional combiner word count : "+ (endTime-startTime) + " milliseconds" );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}