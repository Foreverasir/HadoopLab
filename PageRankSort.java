import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import static org.apache.hadoop.io.WritableComparator.compareBytes;

//class RandomSelectMapper extends Mapper<Object, Text, Text, Text>{
//    private static int currentSize = 0;
//    private Random random = new Random();
//
//    public void map(Object key, Text value, Context context)
//            throws IOException, InterruptedException{
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        Random ran = new Random();
//        while(itr.hasMoreTokens()){
//            currentSize++;
//            if(random.nextInt(currentSize) == ran.nextInt(1)){
//                Text v = new Text(itr.nextToken());
//                context.write(v, v);
//            }
//            else{
//                itr.nextToken();
//            }
//        }
//    }
//
//}
//
//class RandomSelectReducer extends Reducer<Text, Text, Text, Text>{
//
//    public void reduce(Text key, Iterable<Text> values, Context context)
//            throws IOException, InterruptedException{
//
//        for (Text data : values) {
//            context.write(null,data);
//            break;
//        }
//    }
//}
//
//
//class ReducerPartition
//        extends Partitioner<Text, Text> {
//
//    public int getPartition(Text key, Text value ,int numPartitions){
//        return HadoopUtil.getReducerId(value, numPartitions);
//    }
//}
//
//
//class SortMapper extends Mapper<Object, Text, Text, Text> {
//
//    public void map(Object key, Text values,
//                    Context context) throws IOException,InterruptedException {
//        StringTokenizer itr = new StringTokenizer(values.toString());
//        while (itr.hasMoreTokens()) {
//            Text v = new Text(itr.nextToken());
//            context.write(v, v);
//        }
//    }
//
//}
//
//class SortReducer
//        extends Reducer<Text, Text, Text, Text> {
//
//    public void reduce(Text key, Iterable<Text> values,
//                       Context context) throws IOException, InterruptedException {
//
//        for (Text data : values) {
//            context.write(key,data);
//        }
//    }
//}
//
//
//class SortDriver {
//
//    public static void runPivotSelect(Configuration conf,
//                                      Path input,
//                                      Path output) throws IOException, ClassNotFoundException, InterruptedException{
//
//        Job job = new Job(conf, "get pivot");
//        job.setJarByClass(SortDriver.class);
//        job.setMapperClass(RandomSelectMapper.class);
//        job.setReducerClass(RandomSelectReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, input);
//        FileOutputFormat.setOutputPath(job, output);
//        if(!job.waitForCompletion(true)){
//            System.exit(2);
//        }
//    }
//
//    public static void runSort(Configuration conf,
//                               Path input,
//                               Path partition,
//                               Path output) throws IOException, ClassNotFoundException, InterruptedException{
//        Job job = new Job(conf, "sort");
//        job.setJarByClass(SortDriver.class);
//        job.setMapperClass(SortMapper.class);
//        job.setCombinerClass(SortReducer.class);
//        job.setPartitionerClass(ReducerPartition.class);
//        job.setReducerClass(SortReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        HadoopUtil.readPartition(conf, new Path(partition.toString() + "\\part-r-00000"));
//        job.setNumReduceTasks(HadoopUtil.pivots.size());
//        FileInputFormat.addInputPath(job, input);
//        FileOutputFormat.setOutputPath(job, output);
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 3) {
//            System.err.println("Usage: sort <input> <partition> <output>");
//            System.exit(2);
//        }
//
//        Path input = new Path(otherArgs[0]);
//        Path partition = new Path(otherArgs[1]);
//        Path output = new Path(otherArgs[2]);
//
//        HadoopUtil.delete(conf, partition);
//        HadoopUtil.delete(conf, output);
//
//        SortDriver.runPivotSelect(conf,input,partition);
//        SortDriver.runSort(conf,input, partition, output);
//    }
//}

class MyK2 implements WritableComparable<MyK2> {
    private Text value = new Text();


    public void set(String value){
        this.value.set(value);
    }

    @Override
    public String toString()
    {
        return value.toString();
    }
    @Override
    public int compareTo(MyK2 o) {
        double res = Double.parseDouble(o.toString()) - Double.parseDouble(this.toString());
        return res >=0 ? 1 : -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value.readFields(dataInput);
    }

}







public class PageRankSort {
    static Text a = new Text();
    static MyK2 b = new MyK2();
    public static class SortMapper extends Mapper<Object, Text, MyK2, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\t");
            String pageKey = tuple[0];
            double PR = Double.parseDouble(tuple[2]);
            if(tuple.length == 4)
                a.set(pageKey + "\t" + tuple[3]);
            else
                a.set(pageKey);
            b.set(String.valueOf(PR));
            context.write(b, a);
        }
    }

    public static class SortReducer extends Reducer<MyK2, Text, Text, MyK2> {
        @Override
        public void reduce(MyK2 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t : values) {
                context.write(t, key);
            }
        }

    }


}
