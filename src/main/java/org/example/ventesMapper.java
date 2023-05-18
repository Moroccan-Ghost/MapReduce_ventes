package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ventesMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] sale = value.toString().split(" ");
        String city = sale[1]+sale[0].split("-")[2];
        context.write(new Text(city),new DoubleWritable(Double.parseDouble(sale[3])));
    }
}
