package cc.yuerblog.sql;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * 参考spark官方文档支持的选项：https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/DataFrameWriter.html
 *
 * public void orc(String path)
 * Saves the content of the DataFrame in ORC format at the specified path. This is equivalent to:
 *
 *    format("orc").save(path)
 *
 * You can set the following ORC-specific option(s) for writing ORC files:
 *
 * compression (default is the value specified in spark.sql.orc.compression.codec): compression codec to use when saving to file. This can be one of the known case-insensitive shorten names(none, snappy, zlib, and lzo). This will override orc.compress and spark.sql.orc.compression.codec. If orc.compress is given, it overrides spark.sql.orc.compression.codec.
 * Parameters:
 * path - (undocumented)
 * Since:
 * 1.5.0
 */

public class DF2Hive {
    public void run(FileSystem dfs, SparkSession sess) throws IOException {
        String path = "/people.txt";
        String hive = "/hive/people";

        // 加载json为dataframe
        Dataset<Row> df = sess.read().json(path);

        // 因为此刻我没安装hive，所以只能保存为hive外表的数据，需要手动去hive建外表
        // 如果安装了hive，可以saveAsTable()直接保存到hive内表
        df.write().format("orc").mode("overwrite").option("compression", "zlib").save(hive);

        // 因为此刻没装hive，所以也不能用hive表名加载数据，只能直接指定路径
        // 如果装了hive，也可以用table()替换load，可以直接利用hive的元数据来解析表数据
        Dataset<Row> table = sess.read().format("orc").load(hive);
        table.show();

        // 在此不做具体演示
        // 数据也可以save写入hive表分区路径下，然后通过alter table partition让hive元数据发生变更
        // 也可以通过sql直接insert到hive表分区下会更加方便
    }
}
