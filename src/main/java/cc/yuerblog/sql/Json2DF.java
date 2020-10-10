package cc.yuerblog.sql;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Random;

public class Json2DF {
    public void run(FileSystem dfs, SparkSession sess) throws IOException {
        String path = "/people.txt";

        // 写入1000行JSON测试数据
        FSDataOutputStream out =  dfs.create(new Path(path));
        Random rand = new Random();
        for (int i = 0; i < 1000; i++) {
            PersonBean person = new PersonBean();
            person.setName(String.format("person-%d", i));
            person.setAge(1 + rand.nextInt(100));
            String line = JSONObject.toJSONString(person) + "\n";
            out.write(line.getBytes());
        }
        out.close();

        // 读取JSON文件为一个dataframe（Dataset<Row>）
        Dataset<Row> df = sess.read().json(path);
        // 打印schema
        df.printSchema();
        // 打印数据
        df.show();
    }
}
