package cc.yuerblog.sql;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class HiveTest {
    public void run(FileSystem dfs, SparkSession sess) throws IOException {
        // 建hive库
        sess.sql("create database if not exists spark  ");
        // 切库
        sess.sql("use spark");
        // 建表
        sess.sql("create table if not exists people(name string, age int) stored as orc");

        // 加载JSON DataFrame
        Dataset<Row> df = sess.read().json("/people.txt");
        // 查看DataFrame
        df.show();
        // 创建视图
        df.createOrReplaceTempView("people_view");
        // 追加到表里（注意通过select调整视图表的字段顺序与hive表一致）
        sess.sql("insert into table people select name,age from people_view");
    }
}
