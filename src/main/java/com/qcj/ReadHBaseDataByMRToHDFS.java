package com.qcj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * HBase整合MR
 * 通过MR,从 HBase 中读取数据，(分析之后)然后写入 HDFS
 * 读取hbase的数据到HDFS形成为一个结构化的二维数据表
 *
 * 问题报错：
 * 出现Java.io.IOException:NoFileSystemforscheme:hdfs这个问题,
 * 解决方案：
 * 方法一：
 * 加上configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
 * 方法二：
 *  在core-site.xml中加入如下配置
 *  <property>
 * 		<name>fs.hdfs.impl</name>
 * 		<value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
 * 		<description>The FileSystem for hdfs: uris.</description>
 * 	</property>
 *
 * 	参考博客：
 * 	https://www.linuxidc.com/Linux/2014-01/95944.htm
 */
public class ReadHBaseDataByMRToHDFS{
    private static final String TABLE = "user_info";//读取的表名
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","hadoop1");
        Configuration configuration = HBaseConfiguration.create();
        //加载配置文件可以不用加，在resources目录下就行
        /*configuration.addResource("hbase-site.xml");
        configuration.addResource("core-site.xml");
        configuration.addResource("hdfs-site.xml");*/
        //configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ReadHBaseDataByMRToHDFS.class);
        //设置map
        Scan scan = new Scan();
        //参数false,关于添加依赖jar
        TableMapReduceUtil.initTableMapperJob(TABLE,
                scan,
                ReadHBaseDataByMRToHDFS_Mapper.class,
                Text.class,
                NullWritable.class,
                job,
                false);
        //设置reduce个数
        job.setNumReduceTasks(0);
        //输出目录
        FileOutputFormat.setOutputPath(job,new Path("/hbase_mr/result3"));
        boolean isDone = job.waitForCompletion(true);//提交
        System.out.println("是否提交成功："+isDone);
    }
}

/**
 * 参数
 * ImmutableBytesWritable
 * Result ：HBase中的数据每次取出来是一个Result：就是一个rowkey做一个result
 *
 * keyOut:
 * valueOut:
 */
class ReadHBaseDataByMRToHDFS_Mapper extends TableMapper<Text, NullWritable>{
    Text outKey = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        List<Cell> cells = value.listCells();
        //一个cell一条数据 包含一个column
        for (Cell cell:cells) {
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            long timeStamp = cell.getTimestamp();

            outKey.set(rowkey+"\t"+family+"\t"+column+"\t"+timeStamp);
            context.write(outKey,NullWritable.get());
        }
    }

    /**
     * reducer : 用来做统计结果汇总。 一般来说， 只是针对于 聚合类的 操作肯定需要 reducer， 但是如果针对于不是聚合类的操作，
     * 可以不用编写reducer组件
     */
}
