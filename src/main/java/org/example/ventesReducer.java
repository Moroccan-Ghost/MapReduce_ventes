package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ventesReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double total = 0;
        Iterator<DoubleWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            total+=iterator.next().get();
        }
        context.write(key,new DoubleWritable(total));
    }
}
