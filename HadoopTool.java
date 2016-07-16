import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Created by darnell on 7/9/16.
 */
public class HadoopTool {
    static void deleteFile(String filePath, Configuration conf) throws IOException {
        if(filePath == null)
            return;
//        Path path = new Path(filePath);
//        path.getFileSystem(conf).delete(path, true);
        new File(filePath).delete();
    }


    static void createNewFile(File path)
    {
        File parent = path.getParentFile();
        if(parent != null && !parent.exists())
            parent.mkdirs();
        if(!path.exists())
            try {
                path.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }

    }
}
