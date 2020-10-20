package study.hadoop.mapreduce.homework.searchLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import study.hadoop.mapreduce.wordcount.MyReducer;

/**
 * @author Shi Lei
 * @create 2020-10-07
 */
public class SearchLogMain extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = super.getConf();
    Job job = Job.getInstance(conf, "SearchLog");

    job.setJarByClass(SearchLogMain.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[0]));

    job.setMapperClass(SearchLogMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean b = job.waitForCompletion(true);
    return b ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    int run = ToolRunner.run(configuration, new SearchLogMain(), args);
    System.exit(run);
  }
}
