import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, TupleWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text[] wordArray = new Text[2];

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Implementation of you mapper function
            String line = value.toString();
            //line = line.replaceAll("[^a-zA-Z\\d\\s:]", " ");
            line = line.replaceAll("[^a-zA-Z0-9]", " ");
            line = line.trim();
            String[] tokens = line.split(" ");
            Arrays.sort(tokens);
            System.out.println(Arrays.asList(tokens));
            for (int i = 0; i < tokens.length; i++) {
                for (int j = i+1; j < tokens.length; j++) {
                    wordArray[0].set(tokens[i]);
                    wordArray[1].set(tokens[j]);
                    TupleWritable wordTuple = new TupleWritable(wordArray);
                    context.write(wordTuple, one);
                }
            }
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<TupleWritable, IntWritable, TupleWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(TupleWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Implementation of you combiner function
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<TupleWritable, IntWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(TupleWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Implementation of you reducer function
            Text tupleString = new Text();
            Text value = new Text();
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            value.set(String.valueOf(sum));
            StringBuilder builder = new StringBuilder();
            for (Writable writable : key) {
                builder.append(writable.toString()).append(" ");
            }
            tupleString.set(builder.toString());
            context.write(tupleString, value);

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out each edge and its weight
//            for(String neighbor: map.keySet()){
//                String weight = map.get(neighbor).toString();
//                value.set(" " + neighbor + " " + weight);
//                context.write(key, value);
//            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "mah6449_eoo336"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        // set local combiner class
        job.setCombinerClass(TextCombiner.class);
        // set reducer class
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        job.setMapOutputKeyClass(TupleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    public static class WordPair implements Writable{
        String s1;
        String s2;
        public WordPair(String s1, String s2) {
            this.s1 = s1;
            this.s2 = s2;
        }

        @Override
        public String toString() {
            return s1 + " " + s2;
        }

        @Override
        public boolean equals(Object o) {
            // usual equals checks
            WordPair other = (WordPair) o;
            if (this.s1.equals(other.s1) && this.s2.equals(other.s2))
                return true;
            else if (this.s1.equals(other.s2) && this.s2.equals(other.s1))
                return true;
            return false;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeChars(toString());
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            dataInput.readLine();
        }
    }
}


