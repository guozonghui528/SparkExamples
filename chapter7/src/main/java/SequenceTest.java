import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class SequenceTest {

    public static final String output_path = "hdfs://hd-01:9000/seq";
    private static final String[] DATA = { "a", "b", "c", "d"};

    public static void write(String pathStr) throws IOException {

        System.setProperty("HADOOP_USER_NAME","hadoop");
        Configuration conf = new Configuration();
        conf.addResource(new Path("hdfs-site.xml"));
        conf.addResource(new Path("core-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathStr);

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, IntWritable.class);
        Text key = new Text();

        IntWritable value = new IntWritable();
        for(int i = 0; i < DATA.length; i++) {
            key.set(DATA[i]);
            value.set(i);
            System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }

    public static void main(String[] args) {
        try {
            write(output_path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
