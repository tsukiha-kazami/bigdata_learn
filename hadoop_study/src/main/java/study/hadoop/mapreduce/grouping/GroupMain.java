package study.hadoop.mapreduce.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 分组求top 1
 */
public class GroupMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "group");
        job.setJarByClass(GroupMain.class);

        //第一步：读取文件，解析成为key，value对
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path("file:///C:\\project\\study\\bigdata\\hadoop\\learning_data\\mapReduce\\MapReduce直播预习课件\\3、MapReduce直播2次课\\1、数据&代码资料\\6、mapreduce当中的分组求topN\\数据\\orders.txt"));

        //第二步：自定义map逻辑
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //第三步：分区
        job.setPartitionerClass(GroupPartitioner.class);

        //第四步：排序  已经做了

        //第五步：规约  combiner  省掉

        //第六步：分组   自定义分组逻辑
        job.setGroupingComparatorClass(MyGroup.class);

        //第七步：设置reduce逻辑
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //第八步：设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path("file:///C:\\project\\study\\bigdata\\hadoop\\learning_data\\mapReduce\\MapReduce直播预习课件\\3、MapReduce直播2次课\\1、数据&代码资料\\6、mapreduce当中的分组求topN\\out2"));

        //如果设置reduce任务数为多个，必须打包到集群运行
        //mr中reduce个数，默认是1
//        job.setNumReduceTasks(1);
        //job.setNumReduceTasks(3);

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new GroupMain(), args);
        System.exit(run);
    }
}