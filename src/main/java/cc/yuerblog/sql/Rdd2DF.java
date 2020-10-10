package cc.yuerblog.sql;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Rdd2DF {
    private static class Row2Person implements Function<Row, PersonBean> {
        public PersonBean call(Row row) throws Exception {
            String json = row.getString(0);
            PersonBean person = JSONObject.parseObject(json, PersonBean.class);
            return person;
        }
    }
    public void run(FileSystem dfs, SparkSession sess) throws IOException {
        String path = "/people.txt";

        // 加载Dataframe，每一行就1列
        Dataset<Row> df = sess.read().text(path);

        // Dataframe退化为RDD，方便解析为Person类型
        JavaRDD<PersonBean> personRDD = df.javaRDD().map(new Row2Person());

        // 从RDD转回Dataframe，让Dataframe自动反射java bean的字段
        Dataset<Row> personDF = sess.createDataFrame(personRDD, PersonBean.class);

        // 展示数据
        personDF.show();
    }
}
