package cc.yuerblog.sql;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

import java.io.IOException;

public class DF2View {
    private static class MapFunc implements MapFunction<Row, Integer> {
        public Integer call(Row row) throws Exception {
            Long cnt = row.getAs("cnt");
            return cnt.intValue();
        }
    }
    private static class ReduceFunc implements ReduceFunction<Integer> {
        public Integer call(Integer a, Integer b) throws Exception {
            return a + b;
        }
    }

    public void run(FileSystem dfs, SparkSession sess) throws IOException {
        String path = "/people.txt";

        // 加载dataframe
        Dataset<Row> df = sess.read().json(path);

        // 创建view
        df.createOrReplaceTempView("people");

        // 直接查询SQL
        Dataset<Row> result = sess.sql("select age, count(*) as cnt from people group by age");

        // 打印结果，显示全部行
        result.show();

        // 直接基于Dataset进一步处理，处理结果是Dataset<Integer>
        Dataset<Integer> cntRdd = result.map(new MapFunc(), Encoders.INT());
        Integer total = cntRdd.reduce(new ReduceFunc());
        System.out.println("total=" + total);
    }
}
