package study.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.annotations.Test;

/**
 * @author Shi Lei
 * @create 2020-09-29
 */
public class HdfsExample {
  @Test
  public void testHdfsApi() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "hdfs://node01:8020");
    FileSystem fileSystem = FileSystem.get(configuration);
    //创建kkb 目录
    FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ);
    boolean mkdirs = fileSystem.mkdirs(new Path("hdfs://node01:8020/kkb"), fsPermission);
    if (mkdirs) {
      System.out.println("目录创建成功");
    }

    //上傳
    fileSystem.copyFromLocalFile(
        new Path("file:///C:\\project\\study\\bigdata\\hadoop\\learning_data\\a.txt"),
        new Path("hdfs://node01:8020/kkb"));

//
//    //遍历查找
    RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/kkb"), true);
    while (listFiles.hasNext()) {
      LocatedFileStatus status = listFiles.next();
      // 输出详情
      StringBuilder stringBuilder = new StringBuilder();
      // 文件名称
      stringBuilder.append("文件名称：").append(status.getPath().getName()).append("; ");
      // 权限
      stringBuilder.append("权限：").append(status.getPermission()).append("; ");
      // 分组
      stringBuilder.append("分组：").append(status.getGroup()).append("; ");
//      System.out.println(status.getGroup());
      // 获取存储的块信息
      stringBuilder.append("获取存储的块信息：[");
      BlockLocation[] blockLocations = status.getBlockLocations();
      for (BlockLocation blockLocation : blockLocations) {
        // 获取块存储的主机节点
        String[] hosts = blockLocation.getHosts();
        for (String host : hosts) {
          stringBuilder.append(host).append("; ");
        }
      }//end for
      stringBuilder.append("]");
      System.out.println(stringBuilder.toString());
    }
    fileSystem.close();
  }

  @Test
  public void testHdfsIOTest() throws URISyntaxException, IOException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);

    // 2 创建输入流；路径前不需要加file:///，否则报错
    FileInputStream fis = new FileInputStream(new File("e:\\helo.txt"));
    // 3 获取输出流
    FSDataOutputStream fos = fs.create(new Path("hdfs://node01:8020/outresult.txt"));
    // 4 流对拷 org.apache.commons.io.IOUtils
    IOUtils.copy(fis, fos);
    // 5 关闭资源
    IOUtils.closeQuietly(fos);
    IOUtils.closeQuietly(fis);
    fs.close();
  }
}
