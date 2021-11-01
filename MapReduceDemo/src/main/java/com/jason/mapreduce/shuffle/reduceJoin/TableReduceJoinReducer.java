package com.jason.mapreduce.shuffle.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author WangChenHol
 * @date 2021-11-1 17:06
 **/
public class TableReduceJoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean productBean = new TableBean();

        for (TableBean value : values) {
            try {
                if ("order".equals(value.getSourceFlag())) {
                    TableBean temp = new TableBean();
                    BeanUtils.copyProperties(temp, value);
                    orderBeans.add(temp);
                } else {
                    BeanUtils.copyProperties(productBean, value);
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        for (TableBean order : orderBeans) {
            order.setProductName(productBean.getProductName());
            context.write(order, NullWritable.get());
        }
    }
}
