import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by darnell on 7/4/16.
 */
class PageRankLPAIter {
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


            t1.set(pageKey);
            t2.set("|" + tuple[1]);
            context.write(t1, t2);
            if(tuple[1].equals("NULL"))
                return;


            double PR;
            String label;
            String[] linklist = tuple[1].split(" ");
            if (tuple.length != 4) {
                PR = 0.5;
                label = pageKey;
            }
            else {
                PR = Double.parseDouble(tuple[2]);
                label = tuple[3].toString();
            }
            for (String link : linklist) {
                String[] splits = link.split(",");
                assert splits.length == 2;
                String prValue = null;
                try {
                    prValue = String.valueOf(Double.valueOf(splits[1]) * PR);
                }catch (Exception e){
                    System.out.print(e);
                }
                t1.set(splits[0]);
                t2.set(prValue + "," + label);
                context.write(t1, t2);
            }



        }

    }


    static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
        static double damping = 0.0;
        @Override
        public void setup(Context context){
            damping = context.getConfiguration().getDouble("damp", -1.0);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "NULL";
            double pagerank = 0.0;
            Random random = new Random();
            //calculate pagerank
            HashMap<String, Integer> map = new HashMap<>();
            for(Text value : values){
                String str = value.toString();
                if(str.startsWith("|")){
                    links = str.substring(1, str.length());
                    continue;
                }
                String[] temp = str.split(",");
                if(!map.containsKey(temp[1]))
                    map.put(temp[1], 0);
                map.put(temp[1], map.get(temp[1]) + 1);
                if(temp[0] == null)
                    return;
                pagerank += Double.parseDouble(temp[0]);
            }
            pagerank = 1 - damping + damping * pagerank;


            //get label
            String label = "";
            int max = -1;
            for(String t_label : map.keySet()){
                int temp = map.get(t_label);
                if(temp > max){
                    label = t_label;
                    max = temp;
                }
                if(temp == max){
                    int ran = random.nextInt();
                    if(ran >= 0)
                        label = t_label;
                }
            }
            t1.set(links + "\t" + String.valueOf(pagerank) + "\t" + label);
            context.write(key, t1);
        }
    }
}


