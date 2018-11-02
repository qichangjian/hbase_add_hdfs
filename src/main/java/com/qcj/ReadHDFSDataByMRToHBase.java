package com.qcj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 把hdfs上的二维文本数据插入到hbase中
 *
 * 准备：
 * 1.手动创建hbase表student
 *  create "student","info"
 * 2.准备数据 上传数据到hdfs 下的 /student.txt
 * hadoop fs -put student.txt /
 *
 *
 * 原始数据：student.txt二维文本数据
 * hbase的表：四维表：rowkey column:family, qualifier, value, timestamp
 *
 */
public class ReadHDFSDataByMRToHBase {
    private static final String TABLE = "student";
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","hadoop1");
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ReadHDFSDataByMRToHBase.class);

        job.setMapperClass(ReadHDFSDataByMRToHBase_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableReducerJob(
            TABLE,ReadHDFSDataByMRToHBase_Reduce.class,job,null,null,null,null,false
        );
        //hdfs数据路径
        FileInputFormat.setInputPaths(job,"/student.txt");
        boolean isDone = job.waitForCompletion(true);
        System.out.println("上传数据到hbase:"+isDone);
    }
}

class ReadHDFSDataByMRToHBase_Mapper extends Mapper<LongWritable,Text, Text, NullWritable>{
    /**
     * value :   95019,邢小丽,女,19,IS
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}

/**
 * 往hbase表输出数据:必须要更改对应的数据输出组件：  因为默认的数据输出组件：
 *     //   TextOutputFormat
 *     //   LineRecordWriter
 * 参数：
 * keyin valuein valueout
 */
class ReadHDFSDataByMRToHBase_Reduce extends TableReducer<Text,NullWritable,NullWritable>{
    //创建column的列名数组
    String[] columns = new String[]{"name","sex","age","department"};

    // value :   95019,             邢小丽,女,19,IS
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 这一句代码，就是解析一个行数据，得出了五个字段
        String[] fields = key.toString().trim().split(",");
        //第一个字段当rowkey,
        Put put = new Put(fields[0].getBytes());
        //后面四个字段，分别构造四个key-value
        //插入四个key-value就是四个put
        for (int i = 1; i < fields.length; i++) {
            String field = fields[i];//column  value
            String column = columns[i-1];//column名字
            //info是hbase中定义的列簇
            put.addColumn("info".getBytes(),column.getBytes(),field.getBytes());
            context.write(NullWritable.get(),put);
        }
    }
}