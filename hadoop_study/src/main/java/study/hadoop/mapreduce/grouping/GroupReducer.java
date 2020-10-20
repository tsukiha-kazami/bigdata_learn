package study.hadoop.mapreduce.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    /**
     * Order_0000002	Pdt_03	322.8
     * Order_0000002	Pdt_04	522.4
     * Order_0000002	Pdt_05	822.4
     * => 这一组中有3个kv
     * 并且是排序的
     * Order_0000002	Pdt_05	822.4
     * Order_0000002	Pdt_04	522.4
     * Order_0000002	Pdt_03	322.8
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //Order_0000002	Pdt_05	822.4 获得了当前订单中进而最高的商品
        //top1
//        context.write(key, NullWritable.get());

        //top2
        //这样出不了正确结果，只会将同一个key输出两次;结果如下
        /*
        *
            Order_0000001	222.8
            Order_0000001	222.8
            Order_0000002	822.4
            Order_0000002	822.4
            Order_0000003	222.8
            Order_0000003	222.8
        * */
//        for(int i = 0; i < 2; i++){
//            context.write(key, NullWritable.get());
//        }

        //正确的做法：
        int num = 0;
        for(NullWritable value: values) {
            context.write(key, value);
            num++;
            if(num == 2)
                break;
        }
    }
}