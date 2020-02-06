package yin.zhang.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHDFS {

    Configuration conf = null;
    FileSystem fs = null;

    @Before
    public void open() throws IOException {
        conf = new Configuration();
        //Active状态下的NameNode
        conf.set("fs.defaultFS", "hdfs://192.168.186.138:8020");
        //通过这种方式设置java客户端身份
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(conf);
    }


    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/root");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
    }

    @Test
    public void uploadFile() throws IOException {
        // 文件上传路径
        Path path = new Path("/root/pagerank.txt");
        FSDataOutputStream fsDataOutputStream = fs.create(path);

        InputStream is = new BufferedInputStream(new FileInputStream("C:/Users/12421/Desktop/pagerank.txt"));
        IOUtils.copyBytes(is, fsDataOutputStream, conf, true);
    }

    @Test
    public void readFile() throws IOException {
        Path path = new Path("/root/test.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        // 文件对应block信息
        BlockLocation[] blockLocations  = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            System.out.println(blockLocation);
        }

        // 读取文件
        FSDataInputStream fsDataInputStream = fs.open(path);
        System.out.println((char)fsDataInputStream.readByte());

    }


    @After
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}
