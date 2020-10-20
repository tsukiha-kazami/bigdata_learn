package study.hadoop.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shi Lei
 * @create 2020-09-29
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  //第三步：分区   相同key的数据发送到同一个reduce里面去，相同key合并，value形成一个集合

  /**
   * 继承Reducer类之后，覆写reduce方法
   *
   * @param key
   * @param values
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int result = 0;
    for (IntWritable value : values) {
      //将我们的结果进行累加
      result += value.get();
    }
    //继续输出我们的数据
    IntWritable intWritable = new IntWritable(result);
    context.write(key, intWritable);
  }
}