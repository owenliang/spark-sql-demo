package cc.yuerblog;

import cc.yuerblog.sql.Rdd2DF;
import cc.yuerblog.sql.Json2DF;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;

// 理解：Dataset<Row>是Dataframe表格，而非<Row>的Dataset就是高级点的RDD
public class Main {
    public static void main(String []args) {
        try {
            // spark sql session
            SparkSession sess = SparkSession.builder().appName("spark-sql-demo").getOrCreate();
            // hdfs
            FileSystem dfs =  FileSystem.get(new Configuration());

            // hdfs json文本转dataframe
            Json2DF json2DF = new Json2DF();
            json2DF.run(dfs, sess);

            //
            Rdd2DF rdd2DF = new Rdd2DF();
            rdd2DF.run(dfs, sess);
        } catch (Exception e) {

        }
    }
}
