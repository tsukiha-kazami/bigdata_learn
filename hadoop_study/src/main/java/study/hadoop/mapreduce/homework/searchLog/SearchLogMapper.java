package study.hadoop.mapreduce.homework.searchLog;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shi Lei
 * @create 2020-09-29
 */

public class SearchLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //获取我们的一行数据
    String line = value.toString();
    String[] split = line.split("\\s+");
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);
    text.set(split[1]);
    context.write(text, intWritable);
  }
}
