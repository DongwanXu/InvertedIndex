import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class InvertedIndexBigrams {
    public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().toLowerCase().split("\t",2);
            Text docId = new Text(split[0]);
            String words = split[1].replaceAll("[^a-z ]+", " ");
            StringTokenizer stringTokenizer = new StringTokenizer(words);
            String first = stringTokenizer.nextToken();
            while (stringTokenizer.hasMoreTokens()){
                String second = stringTokenizer.nextToken();
                String group = first + " " + second;
                context.write(new Text(group), new Text(docId));
                first = second;
            }
        }
    }
    public static class WordReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap();
            for(Text current: values) {
                String value = current.toString();
                if(map.containsKey(value)) {
                    map.put(value, map.get(value) + 1);
                }else{
                    map.put(value, 1);
                }
            }
            StringBuilder stringBuilder = new StringBuilder("");
            for(String current: map.keySet()) {
                stringBuilder.append(current+ ":" + map.get(current)+ " ");
            }
            context.write(key, new Text(stringBuilder.toString()));
        }
    }
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length != 2) {
            System.out.println("Not enough argument");
        }else{
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration, "inverted index bigrams");
            job.setJarByClass(InvertedIndexBigrams.class);
            job.setMapperClass(InvertedIndexBigrams.WordMapper.class);
            job.setReducerClass(InvertedIndexBigrams.WordReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)? 0 : 1);
        }
    }
}
