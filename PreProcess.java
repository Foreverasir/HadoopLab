import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.net.URI;

public class PreProcess {

    public static class MyMapper extends Mapper<Object, Text, Text, Text>{
        private static HashSet<String> library;
        private static Text nullText;
        private static BufferedReader fr;
        private static HashMap<String, String> aliasTable;

        @Override
        public void setup(Context context) throws IOException {
            URI[] uris  = context.getCacheFiles();
            library = new HashSet<>();
            aliasTable = new HashMap<>();

            nullText = new Text("");
            try {
                Path path = new Path(context.getConfiguration().get("name_list", null));

                fr = new BufferedReader(new FileReader(path.toString()));
//                fr = new BufferedReader(new FileReader(uris[0].getPath()));
                String text = fr.readLine();
                while(text != null) {
                    library.add(text);
                    text = fr.readLine();
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache:" + e);
            }
            for(String str:library){
                UserDefineLibrary.insertWord(str,"nr",1000);
            }

            String aliasFile = context.getConfiguration().get("alias", null);
            if(aliasFile != null) {
//                fr = new BufferedReader(new FileReader(aliasFile));
                fr = new BufferedReader(new FileReader(uris[1].getPath()));
                String text = fr.readLine();
                while (text != null) {
                    String[] splits = text.split(" ");
                    int len = splits.length;
                    for (int i = 0; i < len - 1; i++)
                        if (!aliasTable.containsKey(splits[i]))
                            aliasTable.put(splits[i], splits[len - 1]);
                    text = fr.readLine();
                }
                fr.close();
            }
        }

        @Override
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            HashSet<String> set = new HashSet<>();
            String s = value.toString();
            StringBuilder builder = new StringBuilder();


            List<Term> names = ToAnalysis.parse(s);
            int count = 0;
            for(Term t:names) {
                String str = t.getRealName();
                if(aliasTable.containsKey(str)) {
                    if(library.contains(str))
                        System.out.println();
                    str = aliasTable.get(str);
                    if(!library.contains(str))
                        System.out.println();
                }
                String[] splits = str.split(",");
                for(String name : splits) {
                    if(library.contains(name) && !set.contains(name) ) {
                        builder.append(name).append(" ");
                        set.add(name);
                        count++;
                    }
                }
            }

            if(count == 1 || count == 0)
                return;
            builder.delete(builder.length() - 1, builder.length());
            Text text = new Text(builder.toString());
            context.write(text,nullText);
        }

//        @Override
//        public void cleanup(Context context) throws IOException {
//            Configuration conf = context.getConfiguration();
//            String nameList = conf.get("name_list", null);
//            String alias = conf.get("alias", null);
//            HadoopTool.deleteFile(nameList, conf);
//            HadoopTool.deleteFile(alias, conf);
//
//        }
    }



    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
            for(Text t : values)
                context.write(key,new Text(""));
        }
    }

    public static void deleteDir(File file) throws FileNotFoundException {
        if(!file.exists())
            throw new FileNotFoundException();
        if(file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files)
                deleteDir(f);
        }
    }

    public static void run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        File dir = new File(args[2]);
        if(dir.exists())
            deleteDir(dir);
        Configuration conf=new Configuration();
        String filename = args[1].substring(args[1].lastIndexOf("/") + 1, args[1].length());
        conf.set("name_list", filename);
        if(args.length >= 5) {
            filename = args[3].substring(args[3].lastIndexOf("/") + 1, args[3].length());
            conf.set("alias", filename);
        }
        Job job=new Job(conf,"PreProcess");
        //add cache file
        job.addCacheFile(new Path(args[1]).toUri());
        if(args.length >= 5)
            job.addCacheFile(new Path(args[4]).toUri());

        job.setJarByClass(PreProcess.class);
        job.setNumReduceTasks(Integer.parseInt(args[3]));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        Path outputPath = new Path(args[2]);

        System.out.println(System.getProperty("user.dir"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        outputPath.getFileSystem(conf).delete(outputPath,true);

        boolean res =  job.waitForCompletion(true);
        if(!res){
            System.out.printf("task preprocess return false\n");
            System.exit(-1);

        }
    }


    public static void main(String[] args) throws Exception {
        run(args);
    }

}
