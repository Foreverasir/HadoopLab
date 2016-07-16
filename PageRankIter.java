import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.HashMap;
import java.util.Random;
import java.util.RandomAccess;

/**
 * Created by darnell on 7/4/16.
 */
class PageRankIter {
    private static Text t1 = new Text();
    private static Text t2 = new Text();
    static class PRIterMapper extends Mapper<Object, Text, Text, Text> {
        static double PR_init = 0.0;


        @Override
        public void setup(Context context){
            PR_init = context.getConfiguration().getDouble("PR_init", -1.0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\t");
            String pageKey = tuple[0];

            if(tuple[1].equals("NULL"))
                return;


            double PR;
            String[] linklist = tuple[1].split(" ");
            if (tuple.length != 3)
                PR = 0.5;
            else
                PR = Double.parseDouble(tuple[2]);
            if(!tuple[1].equals("NULL")) {
                for (String link : linklist) {
                    String[] splits = link.split(",");
                    assert splits.length == 2;
                    String prValue = null;
                    try {
                        prValue = String.valueOf(Double.valueOf(splits[1]) * PR);
                    } catch (Exception e) {
                        System.out.print(e);
                    }
                    t1.set(splits[0]);
                    t2.set(prValue);
                    context.write(t1, t2);
                }
            }

            t1.set(pageKey);
            t2.set("|" + tuple[1] + "\t" + String.valueOf(PR));
            context.write(t1, t2);


        }

    }


    static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
        double variance = 0.0;
        double count = 0.0;
        static double damping = 0.0;
        int curTimes = -1;

        @Override
        public void setup(Context context){
            damping = context.getConfiguration().getDouble("damp", -1.0);
            curTimes = Integer.parseInt(context.getConfiguration().get("cur_times", null));
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "NULL";
            double old_pagerank = 0.0;
            double pagerank = 0.0;
            //calculate pagerank
            for(Text value : values){
                String str = value.toString();
                if(str.startsWith("|"))
                {
                    String[] strs = str.substring(1, str.length()).split("\t");
                    links = strs[0];
                    old_pagerank = Double.parseDouble(strs[1]);
                    continue;
                }

                pagerank += Double.parseDouble(str);
            }
            pagerank = 1 - damping + damping * pagerank;

//            variance += Math.pow(pagerank - old_pagerank, 2.0);
//            count++;
            Stat.count.incrementAndGet();
            Stat.lossing.getAndAdd(Math.pow(pagerank - old_pagerank, 2.0));

            t1.set(links + "\t" + String.valueOf(pagerank));
            context.write(key, t1);
        }

        @Override
        public void cleanup(Context context) throws IOException {
//            String output = context.getConfiguration().get("delta", null);
//            assert  output != null;
//
//            String id = String.valueOf(context.getJobID().getId());
//            File file = new File(output + "_" + id);
//            PrintWriter pw = new PrintWriter(new FileWriter(file, true));
//            pw.printf("%.7f\n", variance / count);
//            pw.close();

        }
    }
}

