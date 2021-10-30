package com.jason.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * HDFS客户端
 * 具体步骤:
 * 1.获取客户端对象
 * 2.执行相关命令操作
 * 3.关闭资源
 *
 * @author WangChenHol
 * @date 2021-10-20 14:09
 **/
public class HdfsClient {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        String uri = "hdfs://hadoop102:8020";
        String user = "jason";
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(uri), configuration, user);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    // 创建目录
    @Test
    public void createDiretory() throws IOException {
        // 方式一
//        fileSystem.create(new Path("/hahahahahahh"));
        // 方式二
        fileSystem.mkdirs(new Path("/22222"));
    }

    // 上传文件
    @Test
    public void upload() throws IOException {
        fileSystem.copyFromLocalFile(false, true, new Path("D:\\data\\test-data\\input\\test.txt"), new Path("/test_input_file"));
    }

    // 下载
    @Test
    public void download() throws IOException {
//        fileSystem.copyToLocalFile(false, new Path("/20211019/cust_info.csv"), new Path("D://"), true);
        fileSystem.copyToLocalFile(true, new Path("/20211019/hahaha"), new Path("D://"), true);
    }

    // 移动
    @Test
    public void move() throws IOException {
        fileSystem.rename(new Path("/20211019/rs.xlsx"), new Path("/22222/rs.xlsx"));
    }

    // 更名
    @Test
    public void rename() throws IOException {
        fileSystem.rename(new Path("/22222/rs.xlsx"), new Path("/22222/result.xlsx"));
    }

    // 删除文件
    @Test
    public void deleteFile() throws IOException {
        fileSystem.delete(new Path("/hahahahahahh"), false);
    }

    // 只删除空的目录
    @Test
    public void deleteEmptyDirectory() throws IOException {
        fileSystem.delete(new Path("/own"), false);
    }

    // 删除目录及其下的所有文件
    @Test
    public void deleteDirectory() throws IOException {
        fileSystem.delete(new Path("/result_04"), true);
    }

    // 查看文件详细信息
    @Test
    public void detail() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus next = files.next();
            System.out.println("=========================" + next.getPath() + "=======================");
            System.out.println(next.getPath().getName());
            System.out.println(next.getPermission());
            System.out.println(next.getLen());
            System.out.println(next.getBlockSize());
            System.out.println(next.getGroup());
            System.out.println(next.getModificationTime());
            System.out.println(next.getOwner());
            System.out.println(next.getReplication());
            System.out.println(Arrays.asList(next.getBlockLocations()));
        }
    }

    // 判断是文件还是目录
    @Test
    public void is() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus s : statuses) {
            if (s.isFile()) {
                System.out.println("文件：" + s.getPath().getName());
            } else {
                System.out.println("目录：" + s.getPath().getName());
            }
        }
    }
}
