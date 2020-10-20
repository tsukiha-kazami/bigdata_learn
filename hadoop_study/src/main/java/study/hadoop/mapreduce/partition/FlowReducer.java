package study.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {

    /**
     * 同一个手机号的数据，调用一次reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int donwFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            donwFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        context.write(key, new Text(upFlow + "\t" + donwFlow + "\t" + upCountFlow + "\t" + downCountFlow));
    }
}