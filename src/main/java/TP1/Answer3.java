package TP1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


/**
 * Created by Vivien on 11/10/2016.
 */

public class Answer3 {

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable(){
            super(IntWritable.class);
        }

        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        @Override
        public Writable[] get(){
            return super.get();
        }

        public Writable getWritable(int i){
            return this.get()[i];
        }

    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntArrayWritable> {
        private final static IntWritable two = new IntWritable(2);
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private final static Text male = new Text("male");
        private final static Text female = new Text("female");

        /** MAP FUNCTION
         * I use the gender as the key, and the value is the couple of value [name's gender number, total name number]
         * I use the IntWritable zero, one and two because there is the special case where a name got both genders
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String gender = line.split(";")[1];
            if(gender.equals("m")){
                output.collect(male, new IntArrayWritable(new IntWritable[]{two,two}));
                output.collect(female, new IntArrayWritable(new IntWritable[]{zero,two}));
            }
            else if(gender.equals("f")){
                output.collect(male, new IntArrayWritable(new IntWritable[]{zero,two}));
                output.collect(female, new IntArrayWritable(new IntWritable[]{two,two}));
            }
            else{
                output.collect(male, new IntArrayWritable(new IntWritable[]{one,two}));
                output.collect(female, new IntArrayWritable(new IntWritable[]{one,two}));
            }

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntArrayWritable, Text, FloatWritable> {
        /** REDUCE FUNCTION
         * I sum all the name by gender and all the name
         * I divided the name's gender number by the total name number in order to obtain the percentage of each gender
         */
        public void reduce(Text key, Iterator<IntArrayWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            int gendersum = 0;
            int totalsum = 0;

            while (values.hasNext()) {
                gendersum += ((IntWritable)values.next().getWritable(0)).get();
                totalsum += ((IntWritable)values.next().getWritable(1)).get();

            }

            output.collect(key, new FloatWritable(gendersum*100/totalsum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Answer3.class);
        conf.setJobName("Answer3");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);

        conf.setMapperClass(Answer3.Map.class);
        conf.setReducerClass(Answer3.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }


}


