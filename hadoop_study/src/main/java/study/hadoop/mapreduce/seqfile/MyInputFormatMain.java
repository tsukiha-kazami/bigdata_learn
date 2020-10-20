package study.hadoop.mapreduce.seqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyInputFormatMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "mergeSmallFile");
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job,new Path("file:///C:\\project\\study\\bigdata\\hadoop\\learning_data\\mapReduce\\MapReduce直播预习课件\\3、MapReduce直播2次课\\1、数据&代码资料\\2、自定义InputFormat实现小文件合并\\文件数据"));

        job.setMapperClass(MyInputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //没有reduce。但是要设置reduce的输出的k3   value3 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

//将我们的文件输出成为sequenceFile这种格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job,new Path("file:///C:\\project\\study\\bigdata\\hadoop\\learning_data\\mapReduce\\MapReduce直播预习课件\\3、MapReduce直播2次课\\1、数据&代码资料\\2、自定义InputFormat实现小文件合并\\out1007"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MyInputFormatMain(), args);
        System.exit(run);
    }

}