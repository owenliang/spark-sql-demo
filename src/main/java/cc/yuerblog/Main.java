package cc.yuerblog;

import cc.yuerblog.sql.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;

// 官方教程：https://spark.apache.org/docs/latest/sql-getting-started.html
// 理解：Dataset<Row>是Dataframe表格，而非<Row>的Dataset就是高级点的RDD
// Dataset支持类似SQL的编程API，也能直接执行SQL语句，它需要特殊的对象序列化格式，这样SQL做条件过滤时可以不需要反序列化
public class Main {
    public static void main(String []args) {
        try {
            // spark sql session
            SparkSession sess = SparkSession.builder().appName("spark-sql-demo").enableHiveSupport().getOrCreate();
            // hdfs
            FileSystem dfs =  FileSystem.get(new Configuration());

            // hdfs json文本直接加载为dataframe
            Json2DF json2DF = new Json2DF();
            json2DF.run(dfs, sess);

            // RDD清洗后转Dataframe
            Rdd2DF rdd2DF = new Rdd2DF();
            rdd2DF.run(dfs, sess);

            // dataframe创建为视图表，直接写SQL
            DF2View df2View = new DF2View();
            df2View.run(dfs, sess);

            DF2Hive df2Hive = new DF2Hive();
            df2Hive.run(dfs, sess);

            HiveTest hiveTest = new HiveTest();
            hiveTest.run(dfs, sess);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
